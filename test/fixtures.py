import logging
import glob
import os
import shutil
import subprocess
import tempfile
import uuid

from urlparse import urlparse
from service import ExternalService, SpawnedService
from testutil import get_open_port

class Fixture(object):
    kafka_version = os.environ.get('KAFKA_VERSION', '0.8.0')
    scala_version = os.environ.get("SCALA_VERSION", '2.8.0')
    project_root = os.environ.get('PROJECT_ROOT', os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
    kafka_root = os.environ.get("KAFKA_ROOT", os.path.join(project_root, 'servers', kafka_version, "kafka-src"))
    ivy_root = os.environ.get('IVY_ROOT', os.path.expanduser("~/.ivy2/cache"))

    @classmethod
    def test_resource(cls, filename):
        return os.path.join(cls.project_root, "servers", cls.kafka_version, "resources", filename)

    @classmethod
    def test_classpath(cls):
        # ./kafka-src/bin/kafka-run-class.sh is the authority.
        jars = ["."]

        # 0.8.0 build path, should contain the core jar and a deps jar
        jars.extend(glob.glob(cls.kafka_root + "/core/target/scala-%s/*.jar" % cls.scala_version))

        # 0.8.1 build path, should contain the core jar and several dep jars
        jars.extend(glob.glob(cls.kafka_root + "/core/build/libs/*.jar"))
        jars.extend(glob.glob(cls.kafka_root + "/core/build/dependant-libs-%s/*.jar" % cls.scala_version))

        jars = filter(os.path.exists, map(os.path.abspath, jars))
        return ":".join(jars)

    def kafka_run_class_args(self, *args):
        # ./kafka-src/bin/kafka-run-class.sh is the authority.

        return [
            "java",
            "-Xmx512M",
            "-server",
            "-Dlog4j.configuration=file:%s" % self.render_template("log4j.properties"),
            "-Dcom.sun.management.jmxremote",
            "-Dcom.sun.management.jmxremote.authenticate=false",
            "-Dcom.sun.management.jmxremote.ssl=false",
            "-cp", self.test_classpath(),
        ] + list(args)

    def render_template(self, resource_name):
        source_file = self.test_resource(resource_name)
        target_file = os.path.join(self.tmp_dir, resource_name)

        with open(source_file, "r") as handle:
            template = handle.read()

        with open(target_file, "w") as handle:
            handle.write(template.format(**vars(self)))

        return target_file


class ZookeeperFixture(Fixture):
    @classmethod
    def instance(cls, port = None):
        if "ZOOKEEPER_URI" in os.environ:
            parse = urlparse(os.environ["ZOOKEEPER_URI"])
            (host, port) = (parse.hostname, parse.port)
            fixture = ExternalService('Zookeeper', host, port)
        else:
            (host, port) = ("127.0.0.1", port or get_open_port())
            fixture = cls(host, port)

        fixture.open()
        return fixture

    def __init__(self, host, port):
        self.host = host
        self.port = port

        self.tmp_dir = None
        self.child = None

    def out(self, message):
        logging.info("*** Zookeeper [%s:%d]: %s", self.host, self.port, message)

    def open(self):
        self.tmp_dir = tempfile.mkdtemp()
        self.out("Running local instance...")
        logging.info("  host    = %s", self.host)
        logging.info("  port    = %s", self.port)
        logging.info("  tmp_dir = %s", self.tmp_dir)

        # Configure Zookeeper child process
        self.child = SpawnedService('Zookeeper', self.kafka_run_class_args(
            "org.apache.zookeeper.server.quorum.QuorumPeerMain",
            self.render_template("zookeeper.properties"),
        ))

        # Party!
        self.out("Starting...")
        self.child.start()
        self.child.wait_for(r"Snapshotting")
        self.out("Done!")

    def close(self):
        self.out("Stopping...")
        self.child.stop()
        self.child = None
        self.out("Done!")
        shutil.rmtree(self.tmp_dir)


class KafkaFixture(Fixture):
    @classmethod
    def instance(cls, broker_id, zk_host, zk_port, zk_chroot=None, replicas=1, partitions=2, port = None):
        if zk_chroot is None:
            zk_chroot = "kafka-python_" + str(uuid.uuid4()).replace("-", "_")
        if "KAFKA_URI" in os.environ:
            parse = urlparse(os.environ["KAFKA_URI"])
            (host, port) = (parse.hostname, parse.port)
            fixture = ExternalService('Kafka', host, port)
        else:
            (host, port) = ("127.0.0.1", port or get_open_port())
            fixture = KafkaFixture(host, port, broker_id, zk_host, zk_port, zk_chroot, replicas, partitions)
            fixture.open()
        return fixture

    def __init__(self, host, port, broker_id, zk_host, zk_port, zk_chroot, replicas=1, partitions=2):
        self.host = host
        self.port = port

        self.broker_id = broker_id

        self.zk_host = zk_host
        self.zk_port = zk_port
        self.zk_chroot = zk_chroot

        self.replicas   = replicas
        self.partitions = partitions

        self.tmp_dir = None
        self.child = None
        self.running = False

    def out(self, message):
        logging.info("*** Kafka [%s:%d]: %s", self.host, self.port, message)

    def open(self):
        if self.running:
            self.out("Instance already running")
            return

        self.tmp_dir = tempfile.mkdtemp()
        self.out("Running local instance...")
        logging.info("  host       = %s", self.host)
        logging.info("  port       = %s", self.port)
        logging.info("  broker_id  = %s", self.broker_id)
        logging.info("  zk_host    = %s", self.zk_host)
        logging.info("  zk_port    = %s", self.zk_port)
        logging.info("  zk_chroot  = %s", self.zk_chroot)
        logging.info("  replicas   = %s", self.replicas)
        logging.info("  partitions = %s", self.partitions)
        logging.info("  tmp_dir    = %s", self.tmp_dir)

        # Create directories
        os.mkdir(os.path.join(self.tmp_dir, "logs"))
        os.mkdir(os.path.join(self.tmp_dir, "data"))

        # Configure Kafka child process
        self.child = SpawnedService('Kafka', self.kafka_run_class_args(
            "kafka.Kafka", self.render_template("kafka.properties")
        ))

        # Party!
        self.out("Creating Zookeeper chroot node...")
        proc = subprocess.Popen(self.kafka_run_class_args(
                "org.apache.zookeeper.ZooKeeperMain",
                "-server", "%s:%d" % (self.zk_host, self.zk_port),
                "create", "/%s" % self.zk_chroot, "kafka-python"
            ),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE)

        if proc.wait() != 0:
            self.out("Failed to create Zookeeper chroot node")
            self.out(proc.stdout)
            self.out(proc.stderr)
            raise RuntimeError("Failed to create Zookeeper chroot node")
        self.out("Done!")

        self.out("Starting...")
        self.child.start()
        self.child.wait_for(r"\[Kafka Server %d\], Started" % self.broker_id)
        self.out("Done!")
        self.running = True

    def close(self):
        if not self.running:
            self.out("Instance already stopped")
            return

        self.out("Stopping...")
        self.child.stop()
        self.child = None
        self.out("Done!")
        shutil.rmtree(self.tmp_dir)
        self.running = False
