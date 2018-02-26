import datetime
import socket
import os
import json
import sys
import logging
import subprocess
from collections import defaultdict

from menthol.util import subprocess_run, sanity_check

logger = logging.getLogger(__name__)


def run_with_args(exe, args_mapping, driver_args, config_args, bm_args):
    cmd = [exe]
    # Config args will override driver args
    args = {**driver_args, **bm_args, **config_args,}
    # Benchmark args shouldn't conflicts with config args
    for k in bm_args:
        if k in config_args:
            logger.error(("Argument {} appears both in "
                          "config args: {} "
                          "and benchmark args: {}"
                          ).format(k, config_args[k], bm_args[k]))
            sys.exit(1)
    optional_args = filter(lambda x:not isinstance(x, int),
                           args_mapping.keys())
    positional_args = sorted(filter(lambda x:isinstance(x, int),
                                    args_mapping.keys()))

    for arg in optional_args:
        cmd.append("--{}={}".format(arg, args[args_mapping[arg]]))
    for arg in positional_args:
        cmd.append("{}".format(args[args_mapping[arg]])) 

    result = subprocess_run(cmd,
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE)
    stdout = result.stdout.decode("utf-8")
    stderr = result.stderr.decode("utf-8")
    logger.debug("stdout:\n{}".format(stdout))
    logger.debug("stderr:\n{}".format(stderr))
    return cmd, stdout, stderr


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
                    cmd, stdout, stderr = run_with_args(
                        exe=config.get_cmd(bm),
                        args_mapping=bm.get_mapping(config),
                        driver_args=self.args,
                        config_args=config.args,
                        bm_args = bm.args)
                    stats = bm.parse(stdout, stderr)
                    log = {
                        "cmd": cmd,
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

        log_filename = "{}-{}.json".format(hostname, date_str)
        os.makedirs(self.log_dir, exist_ok=True)
        log_path = os.path.join(self.log_dir, log_filename)

        log = sanity_check()
        log["results"] = self.results

        with open(log_path, "w") as log_file:
            json.dump(log, fp=log_file, indent=4)

        logger.info("Log dumped to {}".format(log_path))
