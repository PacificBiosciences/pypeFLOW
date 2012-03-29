Concurrent Execution
======================

``PypeThreadTaskBase`` provides the base class for task that can
be run concurrently. If a task is built with ``PypeThreadTaskBase``,
it has to be used with ``PypeThreadWorkflow``.  And all other tasks
in the workflow should be ``PypeThreadTaskBase`` objects too. We simply
use python thread for concurrent tasks. Due to the Python GIL, it is
not recommand to have python function for intensive computing as task.
The main purpose for ``PypeThreadTaskBase`` is for building tasks that wrapped
some shell commands for running locally or through a cluster environment.
In the furture, it is possible to add multiprocessing based support
for computation intensive python functions as tasks to avoid the GIL.


