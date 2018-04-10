from menthol import Job
from menthol.job import BashJob, PBSJob


def test_job_empty():
    j = Job()
    assert len(j.env) == 0
    j.set_env({"foo": 1, "bar": 2})
    assert len(j.env) == 2
    assert j.env.get("foo") == 1
    j.set_env({"foo": 42})
    assert len(j.env) == 2
    assert j.env.get("foo") == 42


def test_job_set_env():
    j = Job()
    assert len(j.env) == 0
    j.set_env({"foo": 1, "bar": 2})
    assert len(j.env) == 2
    assert j.env.get("foo") == 1


def test_job_update_env():
    j = Job()
    j.set_env({"foo": 1, "bar": 2})
    j.set_env({"foo": 42})
    assert len(j.env) == 2
    assert j.env.get("foo") == 42


def test_bash_job():
    j = BashJob()
    j.add_cmd(["./a.out"], env={"RUST_TRACE": "DEBUG"})
    j.set_env({"LD_LIBRARY_PATH": "/opt/lib:$LD_LIBRARY_PATH"})
    script = j.generate_script()
    assert script == [
        "#!/bin/bash",
        "export LD_LIBRARY_PATH=/opt/lib:$LD_LIBRARY_PATH",
        "RUST_TRACE=DEBUG ./a.out"
    ]


def test_pbs_job():
    j = PBSJob()
    j.add_cmd(["./a.out"], env={"RUST_TRACE": "DEBUG"})
    j.set_env({"LD_LIBRARY_PATH": "/opt/lib:$LD_LIBRARY_PATH"})
    j.set_ncpus(16)
    j.set_project("c25")
    j.set_queue("normal")
    script = j.generate_script()
    assert script[0] == "#!/bin/bash"
    assert "#PBS -q normal" in script
    assert "#PBS -P c25" in script
    assert "#PBS -l ncpus=16" in script
    assert "export LD_LIBRARY_PATH=/opt/lib:$LD_LIBRARY_PATH" in script
    assert script[-1] == "RUST_TRACE=DEBUG ./a.out"
