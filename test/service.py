import logging
import os
import re
import select
import subprocess
import sys
import threading
import time

__all__ = [
    'ExternalService',
    'SpawnedService',
]

class ExternalService(object):
    def __init__(self, name, host, port):
        print("Using already running service at %s:%d" % (host, port))
        self.name = name
        self.host = host
        self.port = port

    def open(self):
        pass

    def close(self):
        pass


class SpawnedService(threading.Thread):
    def __init__(self, name, args=[]):
        threading.Thread.__init__(self)

        self.name = name
        self.args = args

        self.should_die = threading.Event()

        self.capturing = True
        self.captured_output = []

    def run(self):
        self.child = subprocess.Popen(
            self.args,
            bufsize=1,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            #preexec_fn=os.setpgrp,
        )
        alive = True

        while True:
            self.check_io()

            if self.should_die.is_set():
                self.child.terminate()
                alive = False

            poll_results = self.child.poll()
            if poll_results is not None:
                if not alive:
                    break
                else:
                    raise RuntimeError("Subprocess has died. Aborting. (args=%s)" % ' '.join(str(x) for x in self.args))

    def check_io(self):
        (rds, wds, xds) = select.select([self.child.stdout, self.child.stderr], [], [], 1)

        for fd in rds:
            line = fd.readline()[:-1]
            self.out(line)

            if self.capturing:
                self.captured_output.append(line)

    def out(self, message):
        logging.info("*** %s: %s", self.name, message)

    def wait_for(self, pattern, timeout=10):
        t1 = time.time()

        while True:
            t2 = time.time()
            if t2 - t1 >= timeout:
                try:
                    self.child.kill()
                except:
                    logging.exception("Received exception when killing child process %s", self.name)

                raise RuntimeError("Waiting for %r timed out (%s)" % (pattern, self.name))

            if re.search(pattern, '\n'.join(self.captured_output), re.IGNORECASE) is not None:
                self.capturing = False
                return

            self.captured_output = []
            time.sleep(0.1)

    def start(self):
        threading.Thread.start(self)

    def stop(self):
        self.should_die.set()
        self.join()

