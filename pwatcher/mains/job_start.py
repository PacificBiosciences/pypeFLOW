#!python
"""Wait until file exists, then spawn.

This could be a simple bash script, but then we would have to worry about getting it
installed. This can be run via

    python -m pwatcher.mains.job_start myprog

This should work with nearly any python. Simple stuff.

Note: If anyone replaces this, you must ensure that running this is exactly equivalent
to running the "executable". In other words, no 'mkdir', no 'cd', etc. That will help
with debugging.
"""
import os
import sys
import time

def is_executable(path):
    return os.path.exists(path) and os.access(path, os.X_OK)

def main(prog, executable, timeout=120):
    """Wait up to timeout seconds for the executable to become "executable",
    then exec.
    """
    timeleft = int(timeout)
    while not is_executable(executable):
        if 0 == timeleft:
            raise Exception('Exceeded timeout of %s waiting to run %r' %(
                timeout, executable))
        time.sleep(1)
        timeleft -= 1
    os.execv(executable, (executable,))

if __name__ == "__main__":
    main(*sys.argv)
