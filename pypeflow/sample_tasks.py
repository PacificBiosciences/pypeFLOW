import collections
import logging
from .simple_pwatcher_bridge import (
    PypeLocalFile, makePypeLocalFile, fn,
    PypeTask,
)
LOG = logging.getLogger(__name__)

def task_generic_bash_script(self):
    """Generic script task.
    The script template should be in
    self.parameters['_bash_'].
    The template will be substituted by
    the content of "self" and of "self.parameters".
    (That is a little messy, but good enough for now.)
    """
    self_dict = dict()
    self_dict.update(self.__dict__)
    self_dict.update(self.parameters)
    script_unsub = self.parameters['_bash_']
    script = script_unsub.format(**self_dict)
    script_fn = 'script.sh'
    with open(script_fn, 'w') as ofs:
        ofs.write(script)
    self.generated_script_fn = script_fn


def gen_task(script, inputs, outputs, parameters={}):
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
    parameters['_bash_'] = script
    make_task = PypeTask(
            inputs={k: makePypeLocalFile(v) for k,v in inputs.iteritems()},
            outputs={k: makePypeLocalFile(v) for k,v in outputs.iteritems()},
            parameters=parameters,
            )
    return make_task(task_generic_bash_script)

def create_task_new(i1, o1):
    script = """
cat {input.i1} > {output.o1}
"""
    return gen_task(
            script=script,
            inputs={
                'i1': i1,
            },
            outputs={
                'o1': o1,
            },
            parameters={},
    )

def taskA(self):
    i1 = fn(self.i1)
    o1 = fn(self.o1)
    script = """
set -vex
cat {i1} > {o1}
""".format(**locals())
    script_fn = 'script.sh'
    with open(script_fn, 'w') as ofs:
        ofs.write(script)
    self.generated_script_fn = script_fn

def create_task_old(i1, o1):
    i1 = makePypeLocalFile(i1)
    o1 = makePypeLocalFile(o1)
    parameters = {}
    make_task = PypeTask(
            inputs={
                'i1': i1,
            },
            outputs={
                'o1': o1,
            },
            parameters=parameters,
            )
    return make_task(taskA)
