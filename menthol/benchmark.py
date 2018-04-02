import logging

logger = logging.getLogger(__name__)


class Benchmark(object):
    def __init__(self, name):
        self.name = name
        self.pipelines = []

    def set_driver(self, driver):
        self.driver = driver
