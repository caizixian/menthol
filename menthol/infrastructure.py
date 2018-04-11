import subprocess
import os
import tempfile
import datetime
import socket
import pathlib
import logging
import json

from menthol.job import BashJob, PBSJob

logger = logging.getLogger(__name__)


class Infrastructure(object):
    def __init__(self, name=None):
        if not name:
            date_str = datetime.datetime.now().strftime("%Y-%m-%d-%H%M%S")
            hostname = socket.gethostname()
            self.name = "{}-{}".format(hostname, date_str)
        else:
            self.name = name

    def schedule(self, jobs):
        self.jobs = jobs

    def run(self):
        raise NotImplementedError


class Standalone(Infrastructure):
    def __init__(self, basedir=None):
        super().__init__()
        self.job_class = BashJob
        self.basedir = basedir if basedir else os.path.join(
            os.getcwd(), "results", self.name)
        pathlib.Path(self.basedir).mkdir(parents=True, exist_ok=True)

    def schedule(self, jobs):
        super().schedule(jobs)
        # iterate through benchmarks
        # interleaving configurations
        self.jobs.sort(key=lambda x: (
            x.metadata["benchmark"],
            x.metadata["invocation"],
            x.metadata["configuration"]
        ))
        manifest_filename = os.path.join(self.basedir, "MANIFEST")
        with open(manifest_filename, "a") as manifest_file:
            for j in jobs:
                manifest_file.write("{}\t{}\t{}\t{}\n".format(
                    j.id,
                    json.dumps(j.env),
                    json.dumps(j.cmds),
                    json.dumps(j.metadata)
                ))

    def run(self):
        for j in self.jobs:
            stdout_filename = os.path.join(
                self.basedir,
                j.stdout_filename
            )
            stderr_filename = os.path.join(
                self.basedir,
                j.stderr_filename
            )
            with open(stdout_filename, "w") as stdout_file:
                with open(stderr_filename, "w") as stderr_file:
                    script = tempfile.NamedTemporaryFile(buffering=0)
                    script.write(
                        "\n".join(j.generate_script()).encode("utf-8"))
                    logger.info("Running job: {}".format(j))
                    subprocess.run(
                        ["bash", script.name],
                        stdout=stdout_file,
                        stderr=stderr_file
                    )
                    script.close()


"""
class MultiNodes(Infrastructure):
    def __init__(self, machines):
        super().__init__()
        self.machines = machines
        self.job_class = BashJob


class Raijin(Infrastructure):
    def __init__(self):
        super().__init__()
        self.job_class = PBSJob

    def schedule(self, jobs):
        super().schedule(jobs)
        for j in self.jobs:
            j.

    def run(self):
        for j in self.jobs:
            pbs_filename = "{}.pbs".format(j.short_id)
            with open(pbs_filename, "w") as pbs_file:
                pbs_file.write("\n".join(j.generate_script()))
            subprocess.run(
                ["qsub", ]
            )
"""
