import uuid
from menthol.util import shorten_uuid


class Job(object):
    def __init__(self, name=""):
        self.id = uuid.uuid4()
        self.name = name
        self.cmds = []
        self.env = {}
        self.finished = False
        self.short_id = shorten_uuid(id)
        self.stdout_filename = "{}.o"
        self.stderr_filename = "{}.e"

    def add_cmd(self, cmd, **kwargs):
        self.cmds.append((cmd, kwargs))

    def set_env(self, env):
        self.env.update(env)

    def log_name(self):
        return


class BashJob(Job):
    def __init__(self):
        super().__init__()

    def generate_script(self):
        lines = [
            "#!/bin/bash"
        ]
        for k, v in self.env.items():
            lines.append("export {}={}".format(k, v))
        for cmd, kwargs in self.cmds:
            cmdline = ["{}={}".format(k, v) for k, v in kwargs["env"].items()]
            cmdline.extend(cmd)
            lines.append(" ".join(cmdline))
        return lines


class PBSJob(BashJob):
    def __init__(self):
        super().__init__()
        self.directives = []

    def generate_script(self):
        lines = super().generate_script()
        for d in self.directives:
            lines.insert(1, "#PBS {}".format(d))
        return lines

    def set_project(self, project):
        """The project which you want to charge the jobs resource usage to.
        The default project is specified by the PROJECT environment variable.
        """
        self.directives.append("-P {}".format(project))

    def set_queue(self, queue):
        """Select the queue to run the job in.
        The queues you can use are listed by running nqstat.
        """
        self.directives.append("-q {}".format(queue))

    def set_walltime(self, walltime):
        """The wall clock time limit for the job.
        Time is expressed in seconds as an integer, or in the form:
        [[hours:]minutes:]seconds[.milliseconds]
        System scheduling decisions depend heavily on the walltime request –
        it is always best to make it as accurate as possible.
        """
        self.directives.append("-l walltime={}".format(walltime))

    def set_mem(self, mem):
        """The total memory limit across all nodes for the job – can be
        specified with units of “MB” or “GB” but only integer values can be
        given. There is a small default value.
        Your job will only run if there is sufficient free memory so making an
        accurate memory request will allow your jobs to run sooner.

        A little trial and error may be required to find how much memory your
        jobs are using – nqstat lists jobs' actual usage.
        """
        self.directives.append("-l mem={}".format(mem))

    def set_ncpus(self, ncpus):
        """The number of cpus required for the job to run. The default is 1.

        -l ncpus=N - If the number of CPUs requested, N, is small enough the job
        will run within a single shared memory node.

        If the number of CPUs specified is too large, the job will be
        distributed over multiple nodes. Currently on NCI systems, these larger
        requests are restricted to multiples of 16 for Sandy Bridge and 28 for
        Broadwell nodes.
        """
        self.directives.append("-l ncpus={}".format(ncpus))

    def set_jobfs(self, jobfs):
        """The requested job scratch space. This will reserve disk space, making
        it unavailable for other jobs, so please try not to overestimate your
        needs.

        Any files created in the $PBS_JOBFS directory are automatically removed
        at the end of the job. Ensure that you use integers, and units of MB or
        GB (not case-sensitive).
        """
        self.directives.append("-l jobfs={}".format(jobfs))

    def set_software(self, software):
        """Specifies the licensed software the job requires to run. Refer to
        Software for the specific string to use.

        The string should be a colon separated list (no spaces) if more than one
        software product is used.

        If your job uses licensed software and you do not specify this option
        (or mis-spell the software name), you will probably receive an
        automatically generated email from the license shadowing daemon, and the
        job may be terminated.

        If your job uses unlicensed software, you don't need to use this flag.

        You can check the lsd status and find out more by looking at the license
        status website.
        """
        self.directives.append("-l software={}".format(software))

    def set_other(self, other):
        """Specifies other requirements or attributes of the job. The string
        should be a colon separated list (no spaces) if more than one attribute
        is required. Generally supported attributes are:
            iobound – the job should not share a node with other IO bound jobs

            mdss – the job requires access to the MDSS (usually via the mdss
            command). If MDSS is down, the job will not start.

            gdata1 – the job requires access to the /g/data1. If /g/data1
            filesystem is down, the job will not be started.

            pernodejobfs – the job’s jobfs resource request should be treated
            as a per node request. Normally the jobfs request is for total
            jobfs summed over all nodes allocated to the job (like mem).

            Only relevant to distributed parallel jobs using jobfs.

        You may be asked to specify other options at times to support particular
        needs or circumstances.
        """
        self.directives.append("-l other={}".format(other))

    def set_restartable(self):
        """Specifies your job is restartable, and if the job is executing on a
        node when it crashes, the job will be requeued.

        Both resources used by and resource limits set for the original job will
        carry over to the requeued job. 
        Hence a restartable job must be checkpointing such that it will still be
        able to complete in the remaining walltime should it suffer a node
        crash.

        The default is that jobs are assumed to not be restartable. 
        Note that regardless of the restartable status of a job, time used by
        jobs on crashed nodes is charged against the project they are running
        under, since the onus is on users to ensure minimum waste of resources
        via a checkpointing mechanism which they must build into any
        particularly long running codes.
        """
        self.directives.append("-r y")

    def set_wd(self):
        """Start the job in the directory from which it was submitted.
        Normally jobs are started in the user's home directory.
        """
        self.directives.append("-l wd")
