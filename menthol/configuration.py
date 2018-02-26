#!/usr/bin/env python3


class Configuration(object):
    """Configuration describes how a benchmark should be run.
    It selects the implementation (for example, different implementations) and
    implementation-specific options (for example, whether replay compilation
    should be used). It also derives options from current Run instance, which is
    shared by all configurations (for example, relative heap size, the number of
    threads, etc.)
    """

    def __init__(self, name):
        self.name = name
        self.args = {}
    
    def update_args(self, args):
        self.args.update(args)

    def build(self, benchmark):
        raise NotImplementedError

    def get_cmd(self, benchmark):
        raise NotImplementedError

    def clean(self, benchmark):
        raise NotImplementedError
