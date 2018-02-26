import logging

logger = logging.getLogger(__name__)

class Benchmark(object):
    def set_driver(self, driver):
        self.driver = driver