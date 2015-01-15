import functools
import logging
import os
import random
import socket
import string
import time
import uuid

from six.moves import xrange
from . import unittest
from test.cluster import KafkaCluster

from kafka import KafkaClient
from kafka.common import OffsetRequest

__all__ = [
    'random_string',
    'get_open_port',
    'kafka_versions',
    'KafkaIntegrationTestCase',
    'Timer',
]

def random_string(l):
    s = "".join(random.choice(string.ascii_letters) for i in xrange(l))
    return s.encode('utf-8')

def kafka_versions(*versions):
    def kafka_versions(func):
        @functools.wraps(func)
        def wrapper(self):
            kafka_version = os.environ.get('KAFKA_VERSION')

            if not kafka_version:
                self.skipTest("no kafka version specified")
            elif 'all' not in versions and kafka_version not in versions:
                self.skipTest("unsupported kafka version")

            return func(self)
        return wrapper
    return kafka_versions

def get_open_port():
    sock = socket.socket()
    sock.bind(("", 0))
    port = sock.getsockname()[1]
    sock.close()
    return port

class KafkaIntegrationTestCase(unittest.TestCase):
    create_client = True
    topic = None
    cluster = None

    def setUp(self):
        super(KafkaIntegrationTestCase, self).setUp()
        if not os.environ.get('KAFKA_VERSION'):
            return

        self.topic = self.cluster.random_topics.pop()

        if not self.topic:
            topic = "%s-%s" % (self.id()[self.id().rindex(".") + 1:], random_string(10).decode('utf-8'))
            self.topic = topic.encode('utf-8')

        #if self.create_client:
        #    self.client = self.cluster.client()
        #    #self.client = KafkaClient('%s:%d' % (svc.host, svc.port))

        self.client.ensure_topic_exists(self.topic)

        self._messages = {}

    def tearDown(self):
        super(KafkaIntegrationTestCase, self).tearDown()
        if not os.environ.get('KAFKA_VERSION'):
            return

        if self.create_client:
            self.client.close()

    @classmethod
    def new_cluster(cls, num_brokers = 1, num_topics = 10):
        cluster = KafkaCluster(
            kafka_version = os.environ.get('KAFKA_VERSION'),
            num_brokers = num_brokers,
            log_func = lambda x, y = 1, z =1: 1,
        )

        cluster.create_random_topics(num_topics)

        return cluster

    def current_offset(self, topic, partition):
        offsets, = self.client.send_offset_request([ OffsetRequest(topic, partition, -1, 1) ])
        return offsets.offsets[0]

    def msgs(self, iterable):
        return [ self.msg(x) for x in iterable ]

    def msg(self, s):
        if s not in self._messages:
            self._messages[s] = '%s-%s-%s' % (s, self.id(), str(uuid.uuid4()))

        return self._messages[s].encode('utf-8')

    def key(self, k):
        return k.encode('utf-8')


class Timer(object):
    def __enter__(self):
        self.start = time.time()
        return self

    def __exit__(self, *args):
        self.end = time.time()
        self.interval = self.end - self.start

logging.basicConfig(level=logging.DEBUG)
