import contextlib
import logging
import os

LOG = logging.getLogger()

@contextlib.contextmanager
def cd(newdir):
    prevdir = os.getcwd()
    LOG.debug('CD: %r <- %r' %(newdir, prevdir))
    os.chdir(os.path.expanduser(newdir))
    try:
        yield
    finally:
        LOG.debug('CD: %r -> %r' %(newdir, prevdir))
        os.chdir(prevdir)
def run(script_fn):
    cwd, basename = os.path.split(script_fn)
    with cd(cwd):
        system('bash {}'.format(basename))
def mkdirs(path):
    if not os.path.isdir(path):
        os.makedirs(path)
def system(cmd):
    LOG.info(cmd)
    rc = os.system(cmd)
    if rc:
        raise Exception('{} <- {!r}'.format(rc, cmd))
def touch(myfn):
    cmd = 'touch {}'.format(myfn)
    system(cmd)
