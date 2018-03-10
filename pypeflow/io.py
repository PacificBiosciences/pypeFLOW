from __future__ import absolute_import
from __future__ import unicode_literals
import contextlib
import logging
import os

LOG = logging.getLogger()


def mkdirs(*dirnames):
    for dirname in dirnames:
        if not dirname:
            continue # '' => curdir
        if not os.path.isdir(dirname):
            os.makedirs(dirname)
            if len(dirnames) == 1:
                LOG.info('mkdir -p "{}"'.format(dirnames[0]))


def syscall(call, nocheck=False):
    """Raise Exception in error, unless nocheck==True
    """
    LOG.info('$(%s)' % repr(call))
    rc = os.system(call)
    msg = 'Call %r returned %d.' % (call, rc)
    if rc:
        LOG.warning(msg)
        if not nocheck:
            raise Exception(msg)
    else:
        LOG.debug(msg)
    return rc


def capture(cmd):
    """Return stdout, fully captured.
    Wait for subproc to finish.
    Raise if empty.
    Raise on non-zero exit-code.
    """
    import subprocess
    LOG.info('$ {} >'.format(cmd))
    output = subprocess.check_output(cmd, shell=True, stderr=subprocess.STDOUT)
    if not output:
        msg = '{!r} failed to produce any output.'.format(cmd)
        LOG.warning(msg)
    return output


def symlink(src, name, force=True):
    if os.path.lexists(name):
        os.unlink(name)
    os.symlink(src, name)


def rm(*f):
    syscall('rm -f {}'.format(' '.join(f)))


def touch(*paths):
    msg = 'touch {!r}'.format(paths)
    LOG.debug(msg)
    for path in paths:
        if os.path.exists(path):
            os.utime(path, None)
        else:
            open(path, 'a').close()


def filesize(fn):
    """In bytes.
    Raise if fn does not exist.
    """
    return os.stat(fn).st_size


def exists_and_not_empty(fn):
    if not os.path.exists(fn):
        return False
    if 0 == filesize(fn):
        LOG.debug('File {} is empty.'.format(fn))
        return False
    return True

'''
def substitute(yourdict):
    """
    >>> list(sorted(substitute({'a': '_{b}_', 'b': 'X'}).items()))
    [('a', '_X_'), ('b', 'X')]
    """
    from future.utils import viewitems
    mydict = dict(yourdict)
    for (k, v) in viewitems(yourdict):
        if '{' in v:
            mydict[k] = v.format(**mydict)
    return mydict
'''

@contextlib.contextmanager
def cd(newdir):
    # https://stackoverflow.com/a/24176022
    prevdir = os.getcwd()
    LOG.warning('CD: %r <- %r' % (newdir, prevdir))
    os.chdir(os.path.expanduser(newdir))
    try:
        yield
    finally:
        LOG.warning('CD: %r -> %r' % (newdir, prevdir))
        os.chdir(prevdir)
