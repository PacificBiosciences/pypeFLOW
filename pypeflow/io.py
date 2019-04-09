from __future__ import absolute_import

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
                LOG.debug('mkdir -p "{}"'.format(dirnames[0]))


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


def capture(cmd, nocheck=False):
    """Capture output, maybe checking return-code.
    Return stdout, fully captured.
    Wait for subproc to finish.
    Warn if empty.
    Raise on non-zero exit-code, unless nocheck.
    """
    import subprocess
    LOG.info('$ {} >'.format(cmd))
    proc = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    stdout, stderr = proc.communicate()
    rc = proc.returncode
    if rc:
        msg = '{} <- {!r}:\n{}'.format(rc, cmd, stdout)
        if nocheck:
            LOG.debug(msg)
        else:
            raise Exception(msg)
    assert stderr is None, '{!r} != None'.format(stderr)
    output = stdout
    if not output:
        msg = '{!r} failed to produce any output.'.format(cmd)
        LOG.warning(msg)
    return output


def symlink(src, name, force=True):
    if os.path.lexists(name):
        os.unlink(name)
    os.symlink(src, name)


def fix_relative_symlinks(currdir, origdir, recursive=True, relparent='..'):
    """
    Fix relative symlinks after cp/rsync, assuming they had
    been defined relative to 'origdir'.
    If 'recursive', then perform this in all (non-symlinked) sub-dirs also.
    Skip relative links that point upward shallower than relparent, and warn.
    (Always skip absolute symlinks; we assume those already point to persistent space.)
    """
    if recursive:
        for dn in os.listdir(currdir):
            if not os.path.islink(dn) and os.path.isdir(dn):
                fix_relative_symlinks(os.path.join(currdir, dn), os.path.join(origdir, dn), recursive,
                        os.path.join('..', relparent))
    for fn in os.listdir(currdir):
        fn = os.path.join(currdir, fn)
        if not os.path.islink(fn):
            continue
        oldlink = os.readlink(fn)
        if os.path.isabs(oldlink):
            continue
        if not os.path.normpath(oldlink).startswith(relparent):
            msg = 'Symlink {}->{} seems to point within the origdir tree. This is unexpected. relparent={}'.format(
                fn, oldlink, relparent)
            raise Exception(msg)
            #LOG.warning(msg)
            #continue
        newlink = os.path.relpath(os.path.join(origdir, oldlink), currdir)
        LOG.debug('Fix symlink to {!r} from {!r}'.format(newlink, oldlink))
        symlink(newlink, fn)


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
    LOG.info('CD: %r <- %r' % (newdir, prevdir))
    os.chdir(os.path.expanduser(newdir))
    try:
        yield
    finally:
        LOG.info('CD: %r -> %r' % (newdir, prevdir))
        os.chdir(prevdir)
