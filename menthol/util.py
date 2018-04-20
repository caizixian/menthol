import subprocess
import sys
import logging
import importlib.util
import inspect
import os
import pathlib
from collections import defaultdict

logger = logging.getLogger(__name__)


def pad_output(s, pad, cols=80, file=sys.stderr):
    pad_len = cols - len(s)
    left_len = int(pad_len / 2)
    right_len = pad_len - left_len
    print("{}{}{}".format(pad*left_len, s, pad*right_len), file=file)


def subprocess_run(*args, **kwargs):
    logger.info("Executing {} {}".format(" ".join(args[0]), kwargs))
    return subprocess.run(*args, **kwargs)


def import_by_path(module_name, file_path):
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def drivers_in_module(module):
    from menthol import Driver
    for name, val in inspect.getmembers(module):
        if inspect.isclass(val) and issubclass(val, Driver) and val != Driver:
            yield val


def sanity_check():
    return {
        "uname": os.uname(),
        "cpu_count": os.cpu_count()
    }


def shorten_uuid(uuid):
    return str(uuid).split("-")[0]


def group_by_benchmark(results):
    """
    results: [result] (one result for one set of driver args)
    result: {"driver_args": driver_args, "benchmarks": benchmarks}
    benchmarks: {bm.name(str) -> [log](len: invocatio*configs)}
    log: {
        "cmd": [str], cmd passed to subprocess.run
        "run_kwargs": dict, keyword arguments passed to subprocess.run
        "stdout": str, stdout of execution through subprocess.PIPE
        "stderr": str, stdout of execution through subprocess.PIPE
        "stats": str, benchmark defined stats (MFlops/s, LLC misses, etc.)
        "config": str, canonical string of the config (production-32M, etc.)
    }
    """
    new_results = defaultdict(
        lambda: defaultdict(lambda: defaultdict(list)))
    for result in results:
        driver_args = frozenset(result["driver_args"].items())
        benchmarks = result["benchmarks"]
        for bm_name in benchmarks:
            logs = benchmarks[bm_name]
            for log in logs:
                new_results[bm_name][log["config"]][driver_args].append(
                    log["stats"])
    return new_results


def frozen_dict(d):
    return frozenset(sorted(list(d.items())))


def mkdirp(path):
    pathlib.Path(path).mkdir(parents=True, exist_ok=True)
