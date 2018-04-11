import sys
import logging
import subprocess

from menthol.infrastructure import Standalone

logger = logging.getLogger(__name__)


class Driver(object):
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
                configuration.clean(benchmark)

    def build(self):
        for benchmark in self.benchmarks:
            for configuration in self.configurations:
                configuration.build(benchmark)

    def analyse(self, logdir):
        raise NotImplementedError

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
                    config.realize_job(
                        job=j,
                        invocation=i,
                        benchmark=bm,
                        driver=self
                    )
                    jobs.append(j)
        return jobs

    def end(self):
        pass

    def should_stop(self):
        return True
