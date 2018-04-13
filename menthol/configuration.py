class Configuration(object):
    """Configuration describes how a benchmark should be run.
    It selects the implementation (for example, different implementations) and
    implementation-specific options (for example, whether replay compilation
    should be used).
    """

    def __init__(self, name):
        self.name = name
        self.args = {}
    
    def update_args(self, args):
        self.args.update(args)
