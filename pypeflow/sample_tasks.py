

import logging
from .tasks import gen_task
from .simple_pwatcher_bridge import (
    PypeLocalFile, makePypeLocalFile, fn,
    PypeTask, #Dist,
)
LOG = logging.getLogger(__name__)


def create_task(i1, o1):
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
