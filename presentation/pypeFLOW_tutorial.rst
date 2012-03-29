
pypeFLOW Tutorial
==================

.. image:: escher--unbelievable-527581_1024_768.jpg
   :scale: 40%
   :align: left

-----------------

What is pypeFLOW?
=================

* What is pypeFLOW?  A toolkit to contruct data processing work flow
* Tracking data processing within the Python language

.. image:: pipelines.png
   :scale: 70 %
   :align: center

-----------------

Basic Objects
=============

* data objects (defined in ``pypeflow.data.*``)
* task objects (defined in ``pypeflow.task.*``)
* workflow objects (defined in ``pypeflow.controller.*``)

Analogous to Makefile

.. code-block:: python
    
    @PypeTask( inputs = {'dep1':dep1, 'dep2':dep2},
               outputs = {'target':target} )
    def do_something_to_get_the_target(self, *argv, **kwargv):
        ...

is equivalent to

.. code-block:: make

    target: dep1 dep2
        do_something_to_get_the_target ...

---------------------

Data Objects
============

* ``PypeLocalFile`` is an object representing a reference to local file

.. code-block:: python

    f = PypeLocalFile("file://localhost/home/jchin/test/test.txt")

``f`` is a local file at ``/home/jchin/test/test.txt``

.. code-block:: python

    assert f.localFileName == "/home/jchin/test/test.txt"


------------------------

Task Objects I
=================

* ``PypeTaskBase`` is a base class representing a `task` that converts some 
  input files to some output files.  
* Such `task` is typically constructed by using a decorator (e.g. ``PypeTask``)
  to wrap a function into a ``PypeTaskBase`` objects (or objects of the 
  subclasses of ``PypeTaskBase``)
* One needs to specify the input and output data objects within the decorator.
  The data objects can be referred with the task function that gets wrapped.
* Example:

.. code-block:: python
    
    in_file1 = PypeLocalFile("file://localhost/home/jchin/test/test.txt")

    @PypeTask( inputs = {"in_file1": in_file1, "in_file2": in_file2},
               outputs = {"out_file2": out_file2, "out_file2": out_file2} )
    def task(self, *argv, **kwargv):
        assert self.in_file1.localFileName == "/home/jchin/test/test.txt"

    assert task.in_file1 == in_file1

------------------------

Task Objects II
=================

If you don't like Python's decorator, you can generate tasks by calling the
decorator function directly. This is useful to generate a number of tasks 
programmatically, e.g., using a loop to generate a number of tasks. 

.. code-block:: python

    tasks = []
    def task_func(self, *argv, **kwargv):
        pass

    for i in range(10):
        # task_decorator is a function that takes a function as an input argument
        # and it returns a PypeTaskBase object 
        task_decorator = PypeTask(inputs={"f":inputObjs[i]},
                                  outputs={"g":outputObjs[i]},
                                  URL="task://localhost/task%s" % i) 
        t = task_decorator(task_func)
        tasks.append(t)

-----------------------

Task Objects III
==================

* Different ``*Task`` decorators can wrap different kind of function

    - ``PypeTask``, wrap Python function, run as a Python function

    - ``PypeShellTask``, wrap a string as shell script, run as a Python function
      that executes the shell script

    - other decorators for different purposes can be written as needed (e.g. 
      ``PypeSGETask``)

* One can use ``TaskType`` to control the output task types

    - Simple task type: ``PypeTaskBase``

    - Task type that can be run concurrently within different threads: ``PypeThreadTaskBase``

    
-----------------------

Task Objects IV
==================

.. code-block:: python

    @PypeTask( ..., TaskType = PypeTaskBase)
    def simple_py_func(self, *argv, **kwargv):
        ...

    @PypeTask( ..., TaskType = PypeThreadTaskBase)
    def simple_py_func(self, *argv, **kwargv):
        ...

    @PypeShellTask( ..., TaskType = PypeTaskBase)
    def generate_shell_script(self, *argv, **kwargv):
        ...
        return shell_script_string

    @PypeShellTask( ..., TaskType = PypeThreadTaskBase)
    def generate_shell_script(self, *argv, **kwargv):
        ...
        return shell_script_string

-----------------------

Task Objects V
==================

A ``PythonTaskBase`` is a "callable" object, namely, it implements ``__call__``
method.  When it gets called, it will check the dependency of the input and
output objects and make a decision whether to execute the wrapped function.

.. code-block:: python

    task_decorator = PypeTask(inputs={"f":f},
                              outputs={"g":g}) 
    def task_func(self, *argv, **kwargv):
        do_something()

    # calling task_func() will return True and the original task_func is executed
    # if f is newer than g

    # assuming g does not exist
    task_func() # return True, assuming g is generated
    # run it again
    task_func() # return False, the original task_func is not called, since g is newer than f


    
-----------------------

Workflow Objects 
===================

A ``PypeWorkflow`` object contains a collection of ``PypeDataObjects`` and
``PypeTaskBase`` objects. It calculates the dependency graph and executes all
tasks with the correct order.

* ``PypeWorkflow``: vanilla workflow class, one task at a time
* ``PypeThreadWorkflow``: workflow class that can run tasks concurrently using 
  Python thread library
* ``PypeMPWorkflow``: workflow class that can run tasks concurrently using Python
  multiprocessing library

-----------------------

Workflow Building Pattern  
==========================

* Set up workflow
* Set up a task
    - Set up data objects
    - Define a ``task_func`` to be wrapped
    - Use ``PypeTask`` decorator to create the real ``PypeTaskBase`` object
* Add the task into the workflow (The inputs and outputs will be added automatically)
* Set up more tasks and add them into the workflow
* call ``PypeWorkflow.refreshTargets(target_list)`` to execute the tasks (only task that does not
  satisfy the dependency constrain will be execute)


-----------------------

Put It All Together
==========================

* Code Demo... Link


------------------------

Mutable Data Objects
====================

* Issue

  * Side effect: If a data object (e.g. various gff, cmp.h5 files) is 
    both input and output, we can not use it to calculate dependency. 

  * Such file usually has some "internal states" that affect
    how tasks should be executed

* Solution

  * Be explicit.

  * Introduce "mutableDataObjs"

  * Special data objects to keep track the states
  
  * The standard "inputs" and "outputs" should be "immutable" objects within the
    scope of the code.

-------------------------

Output Collision Detection
==========================

* The dependency graph as a direct acyclic graph helps to find 
  independent tasks that can be run concurrently
* However, in the case that multiple tasks write to the same
  output file, we need to detect "output collision" and do not
  allow tasks that writes to the same to be run concurrently.

-------------------------

Scatter-Gather Pattern
==========================
* Concept of the special "scatter" / "gather" tasks
* Special decorator to generate a set of "scattered tasks"

-------------------------

Debugging Support
==========================

* graphviz dot output
* logging
