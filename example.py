#!/usr/bin/env python
import threading, logging, time

from kafka.client import Kafka081Client

class Producer(threading.Thread):
    daemon = True

    def run(self):
        client = Kafka081Client("localhost:9092")
        producer = client.simple_producer()

        while True:
            producer.send_messages('my-topic', "test")
            producer.send_messages('my-topic', "\xc2Hola, mundo!")

            time.sleep(1)


class Consumer(threading.Thread):
    daemon = True

    def run(self):
        client = Kafka081Client("localhost:9092")
        consumer = client.simple_consumer('test-group', 'my-topic')

        for message in consumer:
            print(message)

def main():
    threads = [
        Producer(),
        Consumer()
    ]

    for t in threads:
        t.start()

    time.sleep(5)

if __name__ == "__main__":
    logging.basicConfig(
        format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
        level=logging.DEBUG
        )
    main()
