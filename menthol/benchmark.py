import logging

logger = logging.getLogger(__name__)


class Benchmark(object):
    def __init__(self, name):
        self.name = name
        self.pipelines = []

    def set_driver(self, driver):
        self.driver = driver

    def build(self, configuration):
        raise NotImplementedError

    def realize_job(self, job, configuration, invocation):
        job.set_metadata({
            "benchmark": self.name,
            "invocation": invocation,
            "configuration": configuration.descr
        })

    def clean(self, configuration):
        raise NotImplementedError