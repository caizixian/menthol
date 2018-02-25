#!/usr/bin/env python3
import datetime
import socket
import os
import json
import sys


class Benchmark(object):
    def __init__(self, invocation, nthread, log_dir):
        self.invocation = invocation
        self.nthread = nthread
        self.kernels = {}
        self.log_dir = log_dir

    def add_kernel(self, kernel):
        self.kernels[kernel.name] = kernel

    def run(self):
        for kernel in self.kernels:
            for i in range(0, self.invocation):
                self.kernels[kernel].invoke(invocation=i,
                                            nthread=self.nthread)

    def dump(self):
        date_str = datetime.datetime.now().strftime("%Y-%m-%d-%H%M%S")
        hostname = socket.gethostname()

        # sanity checks
        log = {
            "uname": os.uname(),
            "cpu_count": os.cpu_count(),
            "kernels": {}
        }

        for kernel in self.kernels:
            log["kernels"][kernel] = self.kernels[kernel].stats

        log_filename = "{}-{}.json".format(hostname, date_str)
        os.makedirs(self.log_dir, exist_ok=True)
        log_path = os.path.join(self.log_dir, log_filename)

        with open(log_path, "w") as log_file:
            json.dump(log, fp=log_file, indent=4)

        print("Log dumped to {}".format(log_path), file=sys.stderr)
