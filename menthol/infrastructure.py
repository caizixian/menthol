import subprocess


class Infrastructure(object):
    def __init__(self, name):
        self.name = name

    def schedule(self, jobs):
        self.jobs = {j.short_id: j for j in jobs}

    def run(self):
        raise NotImplementedError

    def is_finished(self):
        raise NotImplementedError

    def assemble(self):
        raise NotImplementedError


class Standalone(Infrastructure):
    def __init__(self):
        super().__init__("Standalone")

    def schedule(self, jobs):
        super().schedule(jobs)

    def is_finished(self):
        return True


class Raijin(Infrastructure):
    def __init__(self):
        super().__init__("Raijin")


class Moma(Infrastructure):
    def __init__(self, machines, log_folder, master="squirrel"):
        super().__init__("Moma")
        self.machines = machines
        self.master = master
        self.log_folder = log_folder

    def schedule(self, jobs):
        super().schedule(jobs)

    def is_finished(self):
        master_host = "{}.moma".format(self.master)
        ssh = subprocess.Popen(
            ["ssh", master_host, "bash"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            universal_newlines=True,
            bufsize=0
        )
        ssh.stdin.write("cd {}".format(self.log_folder))
        for j in self.jobs:
            if not j.finished:
                ssh.stdin.write("ls {} > /dev/null\n".format(j.stdout_filename))
                ssh.stdin.write("echo {} $?".format(j.stdout_filename))
                ssh.stdin.write("ls {} > /dev/null\n".format(j.stderr_filename))
                ssh.stdin.write("echo {} $?".format(j.stderr_filename))
        ssh.stdin.close()

        for line in ssh.stdout:
            shortid, exitcode = line.strip().split()
            if exitcode == "0":
                self.jobs[shortid].finished = True

        for j in self.jobs:
            if not j.finished:
                return False
        return True

    def assemble(self):
        raise NotImplementedError