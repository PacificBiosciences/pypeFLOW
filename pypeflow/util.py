"""Old stuff
Prefer io.py now.
"""
import logging
import os
from .io import (cd, touch, mkdirs, syscall as system)

LOG = logging.getLogger()

def run(script_fn):
    cwd, basename = os.path.split(script_fn)
    with cd(cwd):
        system('/bin/bash {}'.format(basename))
def rmdirs(path):
    if os.path.isdir(path):
        if len(path) < 20 and 'home' in path:
            LOG.error('Refusing to rm {!r} since it might be your homedir.'.format(path))
            return
        cmd = 'rm -rf {}'.format(path)
        system(cmd)
