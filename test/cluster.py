import abc
import errno
import logging
import os
import os.path
import re
import shutil
import signal
import six
import socket
import subprocess
import tarfile
import tempfile
import threading
import time
import uuid

def slurp(filename, must_exist=True):
    """
        Find the named file, read it into memory, and return it as a string.
    """
    if not must_exist and not os.path.exists(filename):
        return ''

    with open(filename, 'r') as fp:
        return fp.read()


def get_open_port():
    """
    Gets an open port on the system
    """
    sock = socket.socket()
    sock.bind(("", 0))
    port = sock.getsockname()[1]
    sock.close()

    return port


def mkdirp(path):
    """
        Ensure that directory path exists.
        Analogous to mkdir -p

        See http://stackoverflow.com/questions/10539823/python-os-makedirs-to-recreate-path
    """
    try:
        os.makedirs(os.path.expanduser(path))
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise


def download_file(url, dest_path = None, dest_dir = None, skip_if_exists = True):
    if not dest_path and not dest_dir:
        raise TypeError('Either dest_path or dest_dir must be passed in')

    if not dest_path:
        dest_path = os.path.join(dest_dir, os.path.basename(url))

    if not os.path.exists(dest_path) or not skip_if_exists:
        response = six.moves.urllib.request.urlopen(url)
        with open(dest_path, 'wb') as fp:
            fp.write(response.read())

    return dest_path


def extract_tarfile(filename, dest_dir, validate = True, gz = None):
    """
    Sanity checks the tar file to ensure there are no paths which escape.
    In particular, looks for relative and absolute paths.

    Params:
    validate checks for unsafe tar files (member paths beginning with .. or /)
    gz allows overriding the automatic detection of .tar.gz files

    Returns the path to the extracted tar file (including the root paths)
    """
    if gz == False or not filename.endswith('.gz'):
        mode = 'r'
    else:
        mode = 'r:gz'

    mkdirp(dest_dir)
    with tarfile.open(filename, mode) as fp:
        # We make the bold assertion that the first file is the "root directory".
        # This should hold if it is a substring of all member paths.
        extract_dir = fp.getmembers()[0].name

        for member in fp.getmembers():
            if extract_dir and not member.name.startswith(extract_dir):
                extract_dir = ''

            if validate and member.name.startswith('/') or member.name.startswith('..'):
                raise UnsafeTarError(member)

        if len(fp.getmembers()) == 1:
            extract_dir = ''

        fp.extractall(dest_dir)

    return os.path.join(dest_dir, extract_dir)


subprocess_lock = threading.RLock()


def run_cmd(cmdline, env = None, shell = False):
    """
    Executes a command, returns the return code and the merged stdout/stderr contents.
    """
    global subprocess_lock
    try:
        fp = tempfile.TemporaryFile()
        with subprocess_lock:
            child = subprocess.Popen(cmdline,
                env     = env,
                shell   = shell,
                bufsize = 2,
                stdout  = fp,
                stderr  = fp,
            )

        return_code = child.wait()
        fp.seek(0, 0)
        output = fp.read()

        return return_code, output
    except OSError as e:
        if e.errno == errno.ENOENT:
            e.msg += '\n' + ' '.join(cmdline)
        raise


def run_daemon(cmdline, env = None, shell = False):
    """
    Executes a command, returns the subprocess object and the log file
    """
    global subprocess_lock
    try:
        fp = tempfile.NamedTemporaryFile(delete = False)
        with subprocess_lock:
            child = subprocess.Popen(cmdline,
                env     = env,
                shell   = shell,
                bufsize = 2,
                stdout  = fp,
                stderr  = fp,
            )

        return child, fp.name
    except OSError as e:
        if e.errno == errno.ENOENT:
            e.msg += '\n' + ' '.join(cmdline)
        raise

class StartupFailureError(Exception): pass

class AsyncFileTailer(threading.Thread):
    """
    Sets up a thread which reads from a file handle and directs the output to a log function.
    This is particularly useful when multiprocessing.

    Note that wait_for calls start() for you.
    """
    daemon = True

    def __init__(self, filename, log_func):
        super(AsyncFileTailer, self).__init__()

        self.filename = filename
        self.log_func = log_func

    def run(self):
        offset = 0
        mtime = new_mtime = os.stat(self.filename).st_mtime

        while True:
            if os.path.exists(self.filename):
                with open(self.filename, 'r') as fp:
                    fp.seek(offset)

                    for line in fp:
                        line = line[:-1] # Trim the newline
                        self.log_func(line)

                    offset = fp.tell()

            while mtime != new_mtime:
                time.sleep(0.1)
                new_mtime = os.stat(self.filename).st_mtime


class Service(object):
    def __init__(self, name, *args, **kwargs):
        self.name = name
        self.args = args
        self.kwargs = kwargs
        self.log_func = self.kwargs.get('log_func', logging.info)

    def setup_service(self):
        pass

    @abc.abstractmethod
    def cmd(self):
        pass

    def teardown_service(self):
        pass

    def validate_startup(self, filename):
        pass


class SpawnedService(threading.Thread):
    def __init__(self, service):
        super(SpawnedService, self).__init__()
        self.service = service

        self.shutting_down = False
        self.child = None
        self.wait_for_pattern = None
        self.validated = False

    def run(self):
        try:
            self.service.setup_service()
            self.child, self.log_file = run_daemon(*self.service.cmd())
            self.service.validate_startup(self.log_file)
            self.service.log_func('%s -- %s', self.service.name, self.log_file)
            self.validated = True

            while self.child.poll() is None:
                time.sleep(0.1)

            if not self.shutting_down:
                raise RuntimeError("Subprocess has died.")
        finally:
            self.service.teardown_service()

    def signal(self, sig):
        if sig in (signal.SIGTERM, signal.SIGKILL):
            self.shutting_down = True

        self.child.send_signal(sig)

    def kill(self):
        self.signal(signal.SIGKILL)

    def terminate(self):
        self.signal(signal.SIGTERM)

    def pause(self):
        self.signal(signal.SIGSTOP)

    def resume(self):
        self.signal(signal.SIGCONT)

    def wait_for_validation(self, timeout):
        t1 = time.time()

        while not self.validated:
            if time.time() - t1 > timeout:
                raise StartupFailureError(self.service.name)
            time.sleep(0.1)


class SpawnedCluster(object):
    def __init__(self, *args, **kwargs):
        self.args     = args
        self.kwargs   = kwargs
        self.services = {}
        self.log_func = kwargs.get('log_func', logging.info)

        self.setup_cluster()

    def setup_cluster(self):
        pass

    def add_service(self, service, timeout):
        self.services[service.name] = ss = SpawnedService(service)
        ss.start()
        ss.wait_for_validation(timeout)
        setattr(self, service.name, ss)

    def add_services(self, services, timeout):
        end_time = time.time() + timeout

        for service in services:
            self.services[service.name] = SpawnedService(service)
            self.services[service.name].start()

            setattr(self, service.name, self.services[service.name])

        for service in services:
            self.services[service.name].wait_for_validation(end_time - time.time())

    def kill_all(self, termination_timeout):
        self.signal_all(signal.SIGKILL, termination_timeout)

    def stop_all(self, termination_timeout):
        self.signal_all(signal.SIGTERM, termination_timeout)

    def signal_all(self, signal, termination_timeout = None):
        end_time = time.time() + termination_timeout
        for name, ss in self.services.items():
            ss.signal(signal)

        if termination_timeout:
            for name, ss in self.services.items():
                ss.join(time.time() - termination_timeout)
                return False

        return True


class KafkaService(Service):
    def cmd(self):
        return self.gen_kafka_run_cmd(self.jarfile, self.propfile)

    @property
    def config_root(self):
        project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
        return os.path.join(project_root, 'servers', self.kafka_version, 'resources')

    def resource(self, filename):
        return os.path.join(self.config_root, filename)

    def gen_kafka_run_cmd(self, *args):
        result = [os.path.join(self.kwargs['kafka_root'], 'bin', 'kafka-run-class.sh')] + list(args)

        env = os.environ.copy()
        env['KAFKA_LOG4J_OPTS'] = "-Dlog4j.configuration=file:%s" % self.resource("log4j.properties")

        return result, env

    def render_template(self, source_file, target_file, binding):
        with open(source_file, "r") as fp:
            template = fp.read()

        with open(target_file, "w") as fp:
            fp.write(template.format(**binding))

    def validate_string_in_log(self, filename, pattern, timeout):
        end_time = time.time() + timeout
        pattern = re.compile(pattern, re.IGNORECASE)

        while time.time() < end_time:
            if os.path.exists(filename):
                if pattern.search(slurp(filename)):
                    return
                time.sleep(0.1)

        raise Exception("Didn't start {}\n\n{}".format(self.name, slurp(filename, must_exist = False)))


class ZKService(KafkaService):
    jarfile = "org.apache.zookeeper.server.quorum.QuorumPeerMain"

    def setup_service(self):
        self.kafka_version = self.kwargs['kafka_version']
        self.host = self.kwargs.get('host') or '127.0.0.1'
        self.port = self.kwargs.get('port') or get_open_port()
        self.zk_chroot = self.kwargs.get('chroot') or str(uuid.uuid4()).replace("-", "_")
        self.tmpdir = tempfile.mkdtemp()

        # Generate configs
        prop_template = self.resource('zookeeper.properties')
        self.propfile = os.path.join(self.tmpdir, 'zookeeper.properties')
        self.render_template(prop_template, self.propfile, {
            'host': self.host,
            'port': self.port,
            'tmp_dir': self.tmpdir,
        })

    def teardown_service(self):
        shutil.rmtree(self.tmpdir)

    def validate_startup(self, log_file):
        self.validate_string_in_log(log_file, 'binding to port', 5)
        AsyncFileTailer(log_file, self.log_func).start()

        self.log_func('Creating chroot')
        exit_code, output = run_cmd(
            *self.gen_kafka_run_cmd("org.apache.zookeeper.ZooKeeperMain",
                "-server", '{}:{}'.format(self.host, self.port),
                "create", os.path.join('/', self.zk_chroot),
                "kafka-python"
            )
        )

        if exit_code != 0:
            raise RuntimeError("Failed to create ZK chroot")


class KafkaService(KafkaService):
    jarfile = 'kafka.Kafka'

    def setup_service(self):
        self.kafka_version = self.kwargs['kafka_version']
        self.broker_id = self.kwargs['broker_id']
        self.host = self.kwargs.get('host') or '127.0.0.1'
        self.port = self.kwargs.get('port') or get_open_port()
        self.tmpdir = tempfile.mkdtemp()

        # Create directories
        os.mkdir(os.path.join(self.tmpdir, "logs"))
        os.mkdir(os.path.join(self.tmpdir, "data"))

        # Create configs
        prop_template = self.resource("kafka.properties")
        self.propfile = os.path.join(self.tmpdir, "kafka.properties")
        self.render_template(prop_template, self.propfile, {
            'zk_host': self.kwargs['zk'].host,
            'zk_port': self.kwargs['zk'].port,
            'zk_chroot': self.kwargs['zk'].zk_chroot,
            'host': self.host,
            'port': self.port,
            'broker_id': self.broker_id,
            'tmp_dir': self.tmpdir,
            'replicas': self.kwargs.get('replicas', 1),
            'partitions': self.kwargs.get('partitions', 2),
        })

    def teardown_service(self):
        shutil.rmtree(self.tmpdir)

    def validate_startup(self, log_file):
        self.validate_string_in_log(log_file, r"\[Kafka Server {}\], Started".format(self.broker_id), 5)
        AsyncFileTailer(log_file, self.log_func).start()


class KafkaCluster(SpawnedCluster):
    dl_path = os.environ.get('KAFKA_DL_PATH', '/tmp/kafka_bins')

    # These are the "official preferred" releases, per kafka.org
    official_releases = {
        '0.8.0': 'https://archive.apache.org/dist/kafka/0.8.0/kafka_2.8.0-0.8.0.tar.gz',
        '0.8.1': 'https://archive.apache.org/dist/kafka/0.8.1/kafka_2.9.2-0.8.1.tgz',
        '0.8.1.1': 'https://archive.apache.org/dist/kafka/0.8.1.1/kafka_2.9.2-0.8.1.1.tgz',
        '0.8.2.0' : 'https://people.apache.org/~junrao/kafka-0.8.2.0-candidate1/kafka_2.10-0.8.2.0.tgz',
    }

    def setup_cluster(self):
        self.kafka_version            = self.kwargs['kafka_version']
        self.default_topic_partitions = self.kwargs.get('partitions', 1)
        self.default_topic_replicas   = self.kwargs.get('replicas', 2)

        self.download_official_version(self.kafka_version)
        self.add_service(self.zk_svc(), timeout=5)
        self.add_services([ self.broker_svc(x) for x in range(1, self.kwargs['num_brokers']+1) ], timeout=5)

    def zk_svc(self, kafka_version = None):
        return ZKService('zk',
            kafka_root    = os.path.join(self.dl_path, self.kafka_version),
            kafka_version = kafka_version or self.kafka_version,
            log_func      = self.log_func,
        )

    def broker_svc(self, broker_id, kafka_version = None, replicas = None, partitions = None):
        return KafkaService('kafka_{}'.format(broker_id),
            broker_id     = broker_id,
            kafka_root    = os.path.join(self.dl_path, self.kafka_version),
            kafka_version = kafka_version or self.kafka_version,
            zk            = self.zk.service,
            replicas      = replicas or self.default_topic_replicas,
            partitions    = partitions or self.default_topic_partitions,
            log_func      = self.log_func,
        )

    @classmethod
    def download_official_version(cls, kafka_version):
        url = cls.official_releases[kafka_version]
        extract_path = os.path.join(cls.dl_path, kafka_version)

        mkdirp(cls.dl_path)
        if not os.path.isdir(extract_path):
            download_path = download_file(url, dest_dir = cls.dl_path)
            shutil.move(
                extract_tarfile(download_path, cls.dl_path),
                extract_path,
            )

        return extract_path
