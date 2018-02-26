#!/usr/env/bin python3
import logging
import argparse

import sys
from pathlib import Path

from menthol import __VERSION__
from menthol.util import import_by_path, drivers_in_module

logger = logging.getLogger(__name__)

def setup_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument("-v", "--verbose", action="store_true",
                        help="change logging level to DEBUG")
    parser.add_argument("-i", "--invocation", type=int, default=20,
                        help="how many invocation")
    parser.add_argument("--version", action="version",
                        version="menthol {}".format(__VERSION__))
    parser.add_argument("FILE",
                        help="path to discover drivers")
    subparsers = parser.add_subparsers()
    clean = subparsers.add_parser("clean")
    clean.set_defaults(which="clean")
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

    # Handle subcommands
    if not args.get("which"):
        # Default subcommand, which is benchmarking
        for driver_cls in drivers_in_module(mod):
            driver = driver_cls()
            driver.set_invocation(args["invocation"])
            driver.build()
            driver.start()
    elif args.get("which") == "clean":
        # Cleanings
        for driver_cls in drivers_in_module(mod):
            driver = driver_cls()
            driver.clean()


if __name__ == "__main__":
    main()
