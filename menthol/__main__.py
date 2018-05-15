#!/usr/env/bin python3
import logging
import argparse

import sys
from pathlib import Path

from menthol import Driver
from menthol.__version__ import __VERSION__
from menthol.util import import_by_path, drivers_in_module

logger = logging.getLogger(__name__)


def setup_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument("-v", "--verbose", action="store_true",
                        help="change logging level to DEBUG")
    parser.add_argument("-b", "--benchmarks", type=str,
                        help="select benchmarks, separated by comma")
    parser.add_argument("-c", "--configurations", type=str,
                        help="select configurations, separated by comma")                    
    parser.add_argument("--version", action="version",
                        version="menthol {}".format(__VERSION__))
    parser.add_argument("FILE",
                        help="path to discover drivers")
    subparsers = parser.add_subparsers()

    build = subparsers.add_parser("build")
    build.set_defaults(which="build")

    run = subparsers.add_parser("run")
    run.set_defaults(which="run")
    run.add_argument("-i", "--invocation", type=int, default=20,
                     help="how many invocation")

    clean = subparsers.add_parser("clean")
    clean.set_defaults(which="clean")

    analyse = subparsers.add_parser("analyse")
    analyse.set_defaults(which="analyse")
    analyse.add_argument("LOGDIR")
    return parser


def main():
    parsers = setup_parser()
    args = vars(parsers.parse_args())

    # Config root logger
    if args.get("verbose") == True:
        log_level = logging.DEBUG
    else:
        log_level = logging.INFO
    logging.basicConfig(
        format="[%(levelname)s] %(asctime)s %(filename)s:%(lineno)d %(message)s",
        level=log_level)

    file_path = args["FILE"]
    if not Path(file_path).is_file():
        logger.critical("Failed to load {}. No such file.".format(file_path))
        sys.exit(1)
    mod = import_by_path("custom_driver", file_path)
    logger.info("{} loaded".format(file_path))

    for driver_cls in drivers_in_module(mod):
        driver = driver_cls()  # type: Driver
        if args["benchmarks"]:
            driver.prune_benchmark(args["benchmarks"].split(","))
        if args["configurations"]:
            driver.prune_configurations(args["configurations"].split(","))
        # Handle subcommands
        if args.get("which") == "run":
            driver.infrastructure.setup()
            driver.set_invocation(args["invocation"])
            driver.start()
        elif args.get("which") == "clean":
            driver.clean()
        elif args.get("which") == "build":
            driver.build()
        elif args.get("which") == "analyse":
            driver.analyse(args["LOGDIR"])
        else:
            parsers.print_help()


if __name__ == "__main__":
    main()
