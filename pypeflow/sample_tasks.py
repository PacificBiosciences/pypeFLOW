from __future__ import absolute_import
from __future__ import unicode_literals
import logging
from .tasks import gen_task
from .simple_pwatcher_bridge import (
    PypeLocalFile, makePypeLocalFile, fn,
    PypeTask, #Dist,
)
LOG = logging.getLogger(__name__)


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
