

import collections
import logging
import os
import pprint
from .simple_pwatcher_bridge import (PypeTask, Dist)
from . import io

LOG = logging.getLogger(__name__)


def task_generic_bash_script(self):
    """Generic script task.
    The script template should be in
      self.bash_template
    The template will be substituted by
    the content of "self" and of "self.parameters".
    (That is a little messy, but good enough for now.)
    """
    self_dict = dict()
    self_dict.update(self.__dict__)
    self_dict.update(self.parameters)
    script_unsub = self.bash_template
    script = script_unsub.format(**self_dict)
    script_fn = 'script.sh'
    with open(script_fn, 'w') as ofs:
        ofs.write(script)
    self.generated_script_fn = script_fn


def gen_task(script, inputs, outputs, parameters=None, dist=None):
    """
    dist is used in two ways:
    1) in the pwatcher, to control job-distribution
    2) as additional parameters:
      - params.pypeflow_nproc
      - params.pypeflow_mb
    """
    if parameters is None:
        parameters = dict()
    if dist is None:
        dist = Dist()
    LOG.debug('gen_task({}\n\tinputs={!r},\n\toutputs={!r})'.format(
        script, inputs, outputs))
    parameters = dict(parameters) # copy
    parameters['pypeflow_nproc'] = dist.pypeflow_nproc
    parameters['pypeflow_mb'] = dist.pypeflow_mb
    LOG.debug(' parameters={}'.format(
        pprint.pformat(parameters)))
    LOG.debug(' dist.job_dict={}'.format(
        pprint.pformat(dist.job_dict)))
    def validate_dict(mydict):
        "Python identifiers are illegal as keys."
        try:
            collections.namedtuple('validate', list(mydict.keys()))
        except ValueError as exc:
            LOG.exception('Bad key name in task definition dict {!r}'.format(mydict))
            raise
    validate_dict(inputs)
    validate_dict(outputs)
    validate_dict(parameters)
    make_task = PypeTask(
            inputs={k: v for k,v in inputs.items()},
            outputs={k: v for k,v in outputs.items()},
            parameters=parameters,
            bash_template=script,
            dist=dist,
            )
    return make_task(task_generic_bash_script)
