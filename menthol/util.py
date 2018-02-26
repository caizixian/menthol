#!/usr/bin/env python3
import subprocess
import sys
import logging
import importlib.util
import inspect
import os


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
