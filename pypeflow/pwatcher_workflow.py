from .simple_pwatcher_bridge import (
        PypeProcWatcherWorkflow, MyFakePypeThreadTaskBase,
        makePypeLocalFile, fn, PypeTask)
PypeThreadTaskBase = MyFakePypeThreadTaskBase

__all__ = [
        'PypeProcWatcherWorkflow', 'PypeThreadTaskBase',
        'makePypeLocalFile', 'fn', 'PypeTask',
]
