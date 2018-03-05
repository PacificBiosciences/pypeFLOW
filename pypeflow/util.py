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
def rmdirs(path):
    if os.path.isdir(path):
        if len(path) < 20 and 'home' in path:
            LOG.error('Refusing to rm {!r} since it might be your homedir.'.format(path))
            return
        cmd = 'rm -rf {}'.format(path)
        system(cmd)
def system(cmd):
    LOG.info(cmd)
    rc = os.system(cmd)
    if rc:
        raise Exception('cwd: {}\nrc: {} <- {!r}'.format(os.getcwd(), rc, cmd))
def touch(myfn):
    cmd = 'touch {}'.format(myfn)
    system(cmd)
