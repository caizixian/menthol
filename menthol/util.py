#!/usr/bin/env python3
import subprocess
import sys

def pad_output(s, pad, cols=80, file=sys.stderr):
    pad_len = cols - len(s)
    left_len = int(pad_len / 2)
    right_len = pad_len - left_len
    print("{}{}{}".format(pad*left_len, s, pad*right_len), file=file)


def subprocess_run(*args, **kwargs):
    print("> {} {}".format(" ".join(args[0]), kwargs), file=sys.stderr)
    return subprocess.run(*args, **kwargs)