from __future__ import absolute_import
from __future__ import unicode_literals
import collections
import logging
import os
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


def gen_task(script, inputs, outputs, parameters={}, dist=Dist()):
    LOG.info('gen_task({}\n\tinputs={!r},\n\toutputs={!r})'.format(
        script, inputs, outputs))
    parameters = dict(parameters) # copy
    if dist.local:
        parameters['job_type'] = 'local'
    def validate_dict(mydict):
        "Python identifiers are illegal as keys."
        try:
            collections.namedtuple('validate', mydict.keys())
        except ValueError as exc:
            LOG.exception('Bad key name in task definition dict {!r}'.format(mydict))
            raise
    validate_dict(inputs)
    validate_dict(outputs)
    validate_dict(parameters)
    make_task = PypeTask(
            inputs={k: v for k,v in inputs.iteritems()},
            outputs={k: v for k,v in outputs.iteritems()},
            parameters=parameters,
            bash_template=script,
            )
    return make_task(task_generic_bash_script)
