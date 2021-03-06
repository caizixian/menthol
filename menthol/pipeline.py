import logging

logger = logging.getLogger(__name__)

class Pipeline:
    def __init__(self, name):
        self.name = name

    def bind_driver(self, driver):
        self.driver = driver

    def process(self, benchmark, results):
        """
        results: {config_name -> {driver_args -> [log](len: invocatio)}
        log: {
            "cmd": [str], cmd passed to subprocess.run
            "run_kwargs": dict, keyword arguments passed to subprocess.run
            "stdout": str, stdout of execution through subprocess.PIPE
            "stderr": str, stdout of execution through subprocess.PIPE
            "stats": str, benchmark defined stats (MFlops/s, LLC misses, etc.)
            "config": str, canonical string of the config (production-32M, etc.)
        }
        """
        logger.info("Feeding through pipeline {}".format(self.name))
        return results