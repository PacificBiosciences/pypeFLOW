import logging
import logging.config
import os
import string
import io
LOG = logging.getLogger(__name__)
BASH = '/bin/bash'

# This is used by some programs in falcon_kit/mains.
simple_logging_config = """
[loggers]
keys=root

[handlers]
keys=stream

[formatters]
keys=form01,form02

[logger_root]
level=NOTSET
handlers=stream

[handler_stream]
class=StreamHandler
level=${FALCON_LOG_LEVEL}
formatter=form01
args=(sys.stderr,)

[formatter_form01]
format=%(asctime)s - %(name)s - %(levelname)s - %(message)s

[formatter_form02]
format=[%(levelname)s]%(message)s
"""
def setup_simple_logging(FALCON_LOG_LEVEL='DEBUG', **ignored):
    cfg = string.Template(simple_logging_config).substitute(FALCON_LOG_LEVEL=FALCON_LOG_LEVEL)
    logger_fileobj = io.StringIO(cfg)
    defaults = {}
    logging.config.fileConfig(logger_fileobj, defaults=defaults, disable_existing_loggers=False)

def run_bash(script_fn):
    # Assume script was written by this program, so we know it is
    # available in the filesystem.
    # However, we cannot be sure that the execute permission is set,
    # so run it as a script.
    cmd = '{} -vex {}'.format(BASH, script_fn)
    LOG.info('!{}'.format(cmd))
    rc = os.system(cmd)
    if rc:
        raise Exception('{} <- {!r}'.format(rc, cmd))
