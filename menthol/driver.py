import sys
import logging
import subprocess
import os
import json
from collections import defaultdict
from functools import reduce
from copy import deepcopy

from menthol.infrastructure import Standalone
from menthol.util import frozen_dict

logger = logging.getLogger(__name__)


class Driver(object):
    """It also includes options, which is shared by all configurations (for
    example, relative heap size, the number of threads, etc.)
    """
    
    def __init__(self, log_dir, pipelines=None, infrastructure=None):
        self.log_dir = log_dir
        self.benchmarks = []
        self.configurations = []
        self.args = {}
        self.results = []
        self.infrastructure = infrastructure if infrastructure else Standalone()
        self.pipelines = pipelines if pipelines else []
        for pipeline in self.pipelines:
            pipeline.bind_driver(self)

    def set_invocation(self, invocation):
        self.invocation = invocation
        logger.info("Set invocation to {}".format(self.invocation))

    def add_benchmark(self, benchmark):
        self.benchmarks.append(benchmark)
        benchmark.set_driver(self)

    def prune_benchmark(self, benchmarks):
        self.benchmarks = [b for b in self.benchmarks if b.name in benchmarks]

    def add_configuration(self, configuration):
        self.configurations.append(configuration)

    def update_args(self, args):
        self.args.update(args)

    def clean(self):
        for benchmark in self.benchmarks:
            for configuration in self.configurations:
                benchmark.clean(configuration)

    def build(self):
        for benchmark in self.benchmarks:
            for configuration in self.configurations:
                benchmark.build(configuration)

    def analyse(self, logdir):
        logs = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
        with open(os.path.join(logdir, "MANIFEST")) as manifest_file:
            for line in manifest_file:
                cols = line.split("\t")
                uuid = cols[0]
                metadata = json.loads(cols[3])
                with open(os.path.join(logdir,"{}.o".format(uuid))) as stdout_file:
                    stdout = stdout_file.read()
                with open(os.path.join(logdir,"{}.e".format(uuid))) as stderr_file:
                    stderr = stderr_file.read()
                bm = metadata["benchmark"]
                config = metadata["configuration"]
                driver_args = frozen_dict(metadata["driver_args"])
                logs[bm][config][driver_args].append({
                    "stdout": stdout,
                    "stderr": stderr
                })
        for bm in self.benchmarks:
            bm_result = deepcopy(logs[bm.name])
            for config in bm_result:
                for driver_args in bm_result[config]:
                    bm_result[config][driver_args] = [
                        bm.parse(log["stdout"], log["stderr"])
                        for log in bm_result[config][driver_args]
                    ]
                        
            pipelines = bm.pipelines
            reduce(lambda x, y: y.process(bm, x),
                   pipelines,
                   bm_result)

    def start(self):
        if not getattr(self, "invocation"):
            logger.critical("Invocation not set")
            sys.exit(1)
        self.infrastructure.schedule(self.begin())
        self.infrastructure.run()
        self.end()
        while not self.should_stop():
            self.infrastructure.schedule(self.begin())
            self.infrastructure.run()
            self.end()

    def begin(self):
        jobs = []
        for bm in self.benchmarks:
            for config in self.configurations:
                for i in range(0, self.invocation):
                    j = self.infrastructure.job_class()
                    bm.realize_job(j, config, i)
                    jobs.append(j)
        return jobs

    def end(self):
        pass

    def should_stop(self):
        return True
