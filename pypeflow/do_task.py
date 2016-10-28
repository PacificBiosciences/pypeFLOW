#!/usr/bin/env python2.7
from . import do_support
import argparse
import contextlib
import importlib
import inspect
import json
import logging
import os
import pprint
import sys
import time
DONE = 'done'
STATUS = 'status'
TIMEOUT = 0
LOG = logging.getLogger()
DESCRIPTION = """Given a JSON description, call a python-function.
"""
EPILOG = """
The JSON looks like this:
{
    "inputs": {"input-name": "filename"},
    "outputs": {"output-name": "output-filename (relative)"},
    "python_function": "falcon_kit.mains.foo",
    "parameters": {}
}

This program will run on the work host, and it will do several things:
    - Run in CWD.
    - Verify that inputs are available. (Wait til timeout if not.)
    - Possibly, cd to tmpdir and create symlinks from inputs.
    - Run the python-module.
      - Pass a kwd-dict of the union of inputs/outputs/parameters.
      - Ignore return-value. Expect exceptions.
    - Possibly, mv outputs from tmpdir to workdir.
    - Write exit-code into STATUS.
    - Touch DONE on success.
"""
"""
(Someday, we might also support runnable Python modules, or even executables via execvp().)

Note: qsub will *not* run this directly. There is a higher layer.
"""
"""
"""

def get_parser():
    class _Formatter(argparse.RawDescriptionHelpFormatter, argparse.ArgumentDefaultsHelpFormatter):
        pass
    parser = argparse.ArgumentParser(description=DESCRIPTION, epilog=EPILOG,
        formatter_class=_Formatter,
    )
    parser.add_argument('--timeout',
        type=int, default=60,
        help='How many seconds to wait for input files (and JSON) to exist. (default: %(default)s')
    parser.add_argument('--tmpdir',
        help='Root directory to run in. (Sub-dir name will be based on CWD.)')
    parser.add_argument('json_fn',
        help='JSON file, as per epilog.')
    return parser

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

def mkdirs(path):
    if not os.path.isdir(path):
        os.makedirs(path)

def wait_for(fn):
    global TIMEOUT
    LOG.debug('Checking existence of {!r}'.format(fn))
    while not os.path.exists(fn):
        if TIMEOUT > 0:
            time.sleep(1)
            TIMEOUT -= 1
        else:
            raise Exception('Timed out waiting for {!r}'.format(fn))
    assert os.access(fn, os.R_OK), '{!r} not readable'.format(fn)

def get_func(python_function):
    mod_name, func_name = os.path.splitext(python_function)
    func_name = func_name[1:] # skip dot
    mod = importlib.import_module(mod_name)
    func = getattr(mod, func_name)
    return func

class OldTaskRunner(object):
    def __init__(self, inputs, outputs, parameters):
        for k,v in (inputs.items() + outputs.items()):
            setattr(self, k, v)
        self.parameters = parameters
        self.inputs = inputs
        self.outputs = outputs

def run(json_fn, timeout, tmpdir):
    if isinstance(timeout, int):
        global TIMEOUT
        TIMEOUT = timeout
    wait_for(json_fn)
    LOG.debug('Loading JSON from {!r}'.format(json_fn))
    cfg = json.loads(open(json_fn).read())
    LOG.debug(pprint.pformat(cfg))
    for fn in cfg['inputs'].values():
        wait_for(fn)
    python_function_name = cfg['python_function']
    func = get_func(python_function_name)
    try:
        if False:
            kwds = dict()
            kwds.update(cfg['inputs'])
            kwds.update(cfg['outputs'])
            kwds.update(cfg['parameters'])
            func(**kwds)
        else:
            # old way, for now
            self = OldTaskRunner(cfg['inputs'], cfg['outputs'], cfg['parameters'])
            func(self=self)
            script_fn = getattr(self, 'generated_script_fn', None)
            if script_fn is not None:
                do_support.run_bash(script_fn)
    except TypeError:
        # Report the actual function spec.
        LOG.error('For function "{}", {}'.format(python_function_name, inspect.getargspec(func)))
        raise

def main():
    parser = get_parser()
    parsed_args = parser.parse_args(sys.argv[1:])
    try:
        run(**vars(parsed_args))
    except Exception:
        LOG.critical('Error in {} with args={!r}'.format(sys.argv[0], pprint.pformat(vars(parsed_args))))
        raise

if __name__ == "__main__":
    do_support.setup_simple_logging(**os.environ)
    LOG.debug('Running "{}"'.format(' '.join(sys.argv)))
    main()
