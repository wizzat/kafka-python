#!/usr/bin/env python
import logging
import argparse
import sys
import os
import time
import fixtures
import testutil

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('version', choices = [ '0.8.0', '0.8.1' ])
    parser.add_argument('-n', '--num-kafka', type=int, default=1)
    parser.add_argument('-v', '--verbose', action='store_true')
    parser.add_argument('-z', '--zookeeper-port', type=int, default=2181)
    parser.add_argument('-p', '--port', type=int, default=9092)
    parser.add_argument('-t', '--topics')

    args = parser.parse_args()

    os.environ['KAFKA_VERSION'] = args.version
    if args.verbose:
        log_level = logging.INFO
    else:
        log_level = logging.CRITICAL

    # http://stackoverflow.com/questions/1943747/python-logging-before-you-run-logging-basicconfig
    # This lets you run these guys in tests with a different logging conf per runner
    root = logging.getLogger()
    if root.handlers:
        for handler in root.handlers:
            root.removeHandler(handler)

    logging.basicConfig(
        format = '%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
        level = log_level,
    )

    try:
        print 'Starting Zookeeper'
        zk = fixtures.ZookeeperFixture.instance(port = args.zookeeper_port)
        print 'Started Zookeeper'
        print 'Starting Kafka'
        kafkas = [ fixtures.KafkaFixture.instance(x, zk.host, zk.port, port = args.port + x) for x in xrange(args.num_kafka) ]
        if args.topics:
            topics = args.topics.split(',')

            for topic in topics:
                print 'Creating %s' % topic
                testutil.ensure_topic_creation(client, topic)

        print 'Kafka running'

        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        print 'Shutting Kafka down'
        for kafka in kafkas:
            kafka.close()
        print 'Kafka shut down, shutting down Zookeeper'
        zk.close()
        print 'Zookeeper shut down'
