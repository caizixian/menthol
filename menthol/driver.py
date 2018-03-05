import datetime
import socket
import os
import sys
import logging
import subprocess
from collections import defaultdict

import yaml
try:
    from yaml import CLoader as Loader, CDumper as Dumper
except ImportError:
    from yaml import Loader, Dumper

from menthol.util import subprocess_run, sanity_check

logger = logging.getLogger(__name__)


class Driver(object):
    def __init__(self, log_dir):
        self.log_dir = log_dir
        self.benchmarks = []
        self.configurations = []
        self.args = {}
        self.results = []

    def set_invocation(self, invocation):
        self.invocation = invocation
        logger.info("Set invocation to {}".format(self.invocation))

    def add_benchmark(self, benchmark):
        self.benchmarks.append(benchmark)
        benchmark.set_driver(self)

    def add_configuration(self, configuration):
        self.configurations.append(configuration)

    def update_args(self, args):
        self.args.update(args)

    def clean(self):
        for benchmark in self.benchmarks:
            for configuration in self.configurations:
                configuration.clean(benchmark)

    def build(self):
        for benchmark in self.benchmarks:
            for configuration in self.configurations:
                configuration.build(benchmark)

    def start(self):
        if not getattr(self, "invocation"):
            logger.critical("Invocation not set")
            sys.exit(1)
        self.begin()
        self.end()
        while not self.should_stop():
            self.begin()
            self.end()
        self.stop()

    def begin(self):
        result = {
            "driver_args": self.args.copy(),
            "benchmarks": defaultdict(list)
        }
        for bm in self.benchmarks:
            for i in range(0, self.invocation):
                logger.info("Invocation {}".format(i))
                for config in self.configurations:
                    cmd, kwargs = config.get_run(
                        benchmark=bm,
                        driver_args=self.args,
                        config_args=config.args,
                        bm_args=bm.args)
                    kwargs["stdout"] = subprocess.PIPE
                    kwargs["stderr"] = subprocess.PIPE
                    completed_process = subprocess_run(cmd, **kwargs)
                    stdout = completed_process.stdout.decode("utf-8")
                    stderr = completed_process.stderr.decode("utf-8")
                    logger.debug("stdout:\n{}".format(stdout))
                    logger.debug("stderr:\n{}".format(stderr))
                    stats = bm.parse(stdout, stderr)
                    log = {
                        "cmd": cmd,
                        "run_kwargs": kwargs,
                        "stdout": stdout,
                        "stderr": stderr,
                        "stats": stats,
                        "config": config.name
                    }
                    result["benchmarks"][bm.name].append(log)
        self.results.append(result)

    def end(self):
        pass

    def should_stop(self):
        return True

    def stop(self):
        date_str = datetime.datetime.now().strftime("%Y-%m-%d-%H%M%S")
        hostname = socket.gethostname()

        log_filename = "{}-{}.yml".format(hostname, date_str)
        os.makedirs(self.log_dir, exist_ok=True)
        log_path = os.path.join(self.log_dir, log_filename)

        log = sanity_check()
        log["results"] = self.results

        with open(log_path, "w") as log_file:
            yaml.dump(log, log_file)

        logger.info("Log dumped to {}".format(log_path))
