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

    def start(self):
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

    def out(self, message):
        logging.info("*** %s: %s", self.process_name, message)


class ZookeeperFixture(Fixture):
    def __init__(self, port = None):
        self.host = "127.0.0.1"
        self.port = port or get_open_port()
        self.tmp_dir = tempfile.mkdtemp()
        self.process_name = 'Zookeeper [%s:%d]' % (self.host, self.port)

        # Configure Zookeeper child process
        self.child = SpawnedService(self.process_name, self.kafka_run_class_args(
            "org.apache.zookeeper.server.quorum.QuorumPeerMain",
            self.render_template("zookeeper.properties"),
        ))

        self.start()

    def setup_zk_chroot(self):
        chroot = "kafka-python" + str(uuid.uuid4()).replace("-", "_")

        self.out("Creating Zookeeper chroot node...")
        proc = subprocess.Popen(self.kafka_run_class_args(
                "org.apache.zookeeper.ZooKeeperMain",
                "-server", "%s:%d" % (self.host, self.port),
                "create", "/%s" % chroot, "kafka-python"
            ),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE)

        if proc.wait() != 0:
            self.out("Failed to create Zookeeper chroot node")
            self.out(proc.stdout.read())
            self.out(proc.stderr.read())
            raise RuntimeError("Failed to create Zookeeper chroot node")
        self.out("Done!")

        return chroot


class KafkaFixture(Fixture):
    def __init__(self, broker_id, zk_fixture, replicas=1, partitions=2, port = None):
        self.broker_id = broker_id
        self.host = "127.0.0.1"
        self.port = port or get_open_port()
        self.conn_str = '{}:{}'.format(self.host, self.port)
        self.process_name = 'Kafka [%s:%d]' % (self.host, self.port)

        # Config template parameters
        self.replicas = replicas
        self.partitions = partitions
        self.zk_host = zk_fixture.host
        self.zk_port = zk_fixture.port
        self.zk_chroot = zk_fixture.setup_zk_chroot()
        self.tmp_dir = tempfile.mkdtemp()

        # Create directories
        os.mkdir(os.path.join(self.tmp_dir, "logs"))
        os.mkdir(os.path.join(self.tmp_dir, "data"))

        # Configure Kafka child process
        self.child = SpawnedService('Kafka', self.kafka_run_class_args(
            "kafka.Kafka", self.render_template("kafka.properties")
        ))

        self.start()

