
# @author Jason Chin
#
# Copyright (C) 2010 by Jason Chin 
# Copyright (C) 2011 by Jason Chin, Pacific Biosciences
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.

"""

PypeTask: This module provides the PypeTask class and the decorators that can convert 
a regular python funtion into a PypeTask instance. 

"""


import inspect
import hashlib
import logging

import sys
PYTHONVERSION = sys.version_info[:2]
if PYTHONVERSION == (2,5):
    import simplejson as json
else:
    import json

import os
import shlex

from PypeCommon import * 
from PypeData import FileNotExistError

class TaskFunctionError(PypeError):
    pass

TaskInitialized = "TaskInitialized"
TaskDone = "TaskDone"
TaskContinue = "TaskContinue"
TaskFail = "TaskFail"

class PypeTaskBase(PypeObject):
    """
    Represent a PypeTask. Subclass it to for different kind of
    task.
    """

    supportedURLScheme = ["task"]

    def __init__(self, URL, *argv, **kwargv):

        """
        Constructor of a PypeTask.
        """

        PypeObject.__init__(self, URL, **kwargv)

        self._argv = argv
        self._kwargv = kwargv
        self._taskFun = kwargv['_taskFun']
        self._referenceMD5 = None
        self._status = TaskInitialized
        self._log = logging.Logger('task')


        for defaultAttr in ["inputDataObjs", "outputDataObjs", "parameters"]:
            if defaultAttr not in self.__dict__:
                self.__dict__[defaultAttr] = {}

        # "input" and "output" short cut
        if "input" in kwargv:
            self.inputDataObjs.update(kwargv["input"])
            del kwargv["input"]

        if "output" in kwargv:
            self.outputDataObjs.update(kwargv["output"])
            del kwargv["output"]

        #the keys in inputDataObjs/outputDataObjs/parameters will become a task attribute 
        for defaultAttr in ["inputDataObjs", "outputDataObjs", "parameters"]:
            vars(self).update(self.__dict__[defaultAttr]) 

        self._codeMD5digest = kwargv["_codeMD5digest"]
        self._paramMD5digest = kwargv["_paramMD5digest"]
        self._compareFunctions = [ timeStampCompare ]

        self._updateRDFGraph()
        
    def setInputs( self, inputDataObjs ):
        self.inputDataObjs = inputDataObjs
        vars(self).update( inputDataObjs )
        
    def setOutputs( self, outputDataObjs ):
        self.outputDataObjs = outputDataObjs
        vars(self).update( outputDataObjs )

    def setReferenceMD5(self, md5Str):
        self._referenceMD5 = md5Str

    def _getRunFlag(self):

        """
        Determine whether the PypeTask should be run. It can be overridden in
        subclass to allow more flexible rules.
        """

        runFlag = False
        if self._referenceMD5 != None and self._referenceMD5 != self._codeMD5digest:
            self._referenceMD5 = self._codeMD5digest
            self._log.debug("%s will run due to a change in the reference MD5 digest." % self.URL)
            runFlag = True
        if runFlag == False:
            runFlag = any( [ f(self.inputDataObjs, self.outputDataObjs, self.parameters) for f in self._compareFunctions] )
            if runFlag:
                self._log.debug("%s will run due to a change in inputs or outputs." % self.URL)
            else:
                self._log.debug("%s will not run." % self.URL)

        return runFlag

    def _runTask(self, *argv, **kwargv):

        """ 
        The method to run the decorated function _taskFun(). It is called through __call__() of
        the PypeTask object and it should never be called directly

        TODO: the arg porcessing is still a mess, need to find a better way to do this 
        """
        
        if PYTHONVERSION == (2,5):
            (args, varargs, varkw, defaults)  = inspect.getargspec(self._taskFun)
            #print  (args, varargs, varkw, defaults)
        else:
            argspec = inspect.getargspec(self._taskFun)
            (args, varargs, varkw, defaults) = argspec.args, argspec.varargs, argspec.keywords, argspec.defaults

        if varkw != None:
            self._taskFun(*argv, **kwargv)
        elif varargs != None:
            self._taskFun(*argv)
        elif len(args) != 0:
            nkwarg = {}
            if defaults != None:
                defaultArg = args[-len(defaults):]
                for a in defaultArg:
                    nkwarg[a] = kwargv[a]
                self._taskFun(*argv, **nkwarg)
            else:
                self._taskFun(self)
        else:
            self._taskFun()


    def _updateRDFGraph(self):
        graph = self._RDFGraph = Graph()
        for k,v in self.__dict__.iteritems():
            if k == "URL": continue
            if k[0] == "_": continue
            if k in ["inputDataObjs", "outputDataObjs", "parameters"]:
                if k == "inputDataObjs":
                    for ft, f in v.iteritems():
                        graph.add( (URIRef(self.URL), pypeNS["prereq"], URIRef(f.URL) ) )
                elif k == "outputDataObjs":
                    for ft, f in v.iteritems():
                        graph.add( (URIRef(f.URL), pypeNS["prereq"], URIRef(self.URL) ) )
                elif k == "parameters":
                    graph.add( (URIRef(self.URL), pypeNS["hasParameters"], Literal(json.dumps(v)) ) )
            
                continue

            if k in self.inputDataObjs:
                graph.add( ( URIRef(self.URL), pypeNS["inputDataObject"], URIRef(v.URL) ) )
                continue

            if k in self.outputDataObjs:
                graph.add( ( URIRef(self.URL), pypeNS["outputDataObject"], URIRef(v.URL) ) )
                continue

            if hasattr(v, "URL"):
                graph.add( ( URIRef(self.URL), pypeNS[k], URIRef(v.URL) ) )

            graph.add(  ( URIRef(self.URL), pypeNS["codeMD5digest"], Literal(self._codeMD5digest) ) )
            graph.add(  ( URIRef(self.URL), pypeNS["parameterMD5digest"], Literal(self._paramMD5digest) ) )
    
    def __call__(self, *argv, **kwargv):
        
        """
        Determine whether a task should be run when called. If the dependency is
        not satisified then the _taskFun() will be called to generate the output data objects.
        """
        
        argv = list(argv)
        argv.extend(self._argv)
        kwargv.update(self._kwargv)

        inputDataObjs = self.inputDataObjs
        # need the following loop to force the stupid Islon to update the metadata in the directory
        # otherwise, the file would be appearing as non-existence... sigh, this is a >5 hours hard earned hacks
        for o in inputDataObjs.values():
            d = os.path.dirname(o.localFileName)
            try:
                os.listdir(d) 
            except OSError:
                pass

        outputDataObjs = self.outputDataObjs
        parameters = self.parameters

        runFlag = self._getRunFlag()
            
        if runFlag == True:

            self._runTask(*argv, **kwargv)

            if self.inputDataObjs != inputDataObjs or self.parameters != parameters:
                raise TaskFunctionError("The 'inputDataObjs' and 'parameters' should not be modified in %s" % self.URL)

            self._updateRDFGraph() #allow the task function to modify the output list if necessary
        
        if any([o.exists == False for o in self.outputDataObjs.values()]):
            self._status = TaskFail
        else:
            self._status = TaskDone

    def finalize(self): 
        """ 
        This method is intended to be overriden by subclass to provide extra processing that is not 
        directed related to the processing the input and output data. For the thread workflow, this
        method will be called in the main thread after a take is finished regardless the job status.
        """
        pass

class PypeThreadTaskBase(PypeTaskBase):

    """
    Represent a PypeTask that can be run within a thread. 
    Subclass it to for different kind of task.
    """

    def __init__(self, URL, *argv, **kwargv):
        PypeTaskBase.__init__(self, URL, *argv, **kwargv)

    def setMessageQueue(self, q):
        self._queue = q

    def __call__(self, *argv, **kwargv):

        """
        Similar to the PypeTaskBase.__call__(), but it provide some machinary to pass information
        back to the main thread that run this task in a sepearated thread through the standard python
        queue from the Queue module.
        """

        try:
            runFlag = self._getRunFlag()
        except TaskFunctionError:
            self._status = TaskFail
            self._queue.put( (self.URL, "fail") )
            return
        except FileNotExistError:
            self._status = TaskFail
            self._queue.put( (self.URL, "fail") )
            return

        self._queue.put( (self.URL, "started, runflag: %d" % runFlag) )

        PypeTaskBase.__call__(self, *argv, **kwargv)

        # need the following loop to force the stupid Islon to update the metadata in the directory
        # otherwise, the file would be appearing as non-existence... sigh, this is a >5 hours hard earned hacks
        for o in self.outputDataObjs.values():
            d = os.path.dirname(o.localFileName)
            try:
                os.listdir(d) 
            except OSError:
                pass

        if any([o.exists == False for o in self.outputDataObjs.values()]):
            self._status = TaskFail
            self._queue.put( (self.URL, "fail") )
        else:
            self._status = TaskDone
            self._queue.put( (self.URL, "done") )

class PypeDistributiableTaskBase(PypeThreadTaskBase):

    """
    Represent a PypeTask that can be run within a thread or submit to
    a grid-engine like job scheduling system. 
    Subclass it to for different kind of task.
    """

    def __init__(self, URL, *argv, **kwargv):
        PypeTaskBase.__init__(self, URL, *argv, **kwargv)
        self.distributed = True


def PypeTask(*argv, **kwargv):

    """
    A decorator that converts a function into a PypeTaskBase object.

    >>> import os 
    >>> from PypeData import PypeLocalFile, makePypeLocalFile, fn
    >>> from PypeTask import *
    >>> fin = makePypeLocalFile("test/testfile_in", readOnly=False)
    >>> fout = makePypeLocalFile("test/testfile_out", readOnly=False)
    >>> @PypeTask(output={"test_out":fout},
    ...           input={"test_in":fin},
    ...           parameters={"a":'I am "a"'}, **{"b":'I am "b"'})
    ... def test(self):
    ...     print test.test_in.localFileName
    ...     print test.test_out.localFileName
    ...     os.system( "touch %s" % fn(test.test_out) )
    ...     print self.test_in.localFileName
    ...     print self.test_out.localFileName
    ...     pass
    >>> type(test) 
    <class 'PypeTask.PypeTaskBase'>
    >>> test.test_in.localFileName
    'test/testfile_in'
    >>> test.test_out.localFileName
    'test/testfile_out'
    >>> os.system( "rm %s; touch %s" %  (fn(fout), fn(fin))  )
    0
    >>> timeStampCompare(test.inputDataObjs, test.outputDataObjs, test.parameters)
    True
    >>> print test._getRunFlag()
    True
    >>> test()
    test/testfile_in
    test/testfile_out
    test/testfile_in
    test/testfile_out
    >>> timeStampCompare(test.inputDataObjs, test.outputDataObjs, test.parameters)
    False
    >>> print test._getRunFlag()
    False
    >>> test() #return nothing, since the test_out is newer than the test_in
    >>> print test.a
    I am "a"
    >>> print test.b
    I am "b"
    >>> os.system( "rm %s; touch %s" %  (fn(fout), fn(fin))  )
    0
    >>> # test PypeTask.finalize()
    >>> from PypeController import PypeWorkflow
    >>> wf = PypeWorkflow()
    >>> wf.addTask(test)
    >>> def finalize(self):
    ...     def f():
    ...         print "in finalize:", self._status
    ...     return f
    >>> test.finalize = finalize(test)  # For testing only. Please don't do this in your code. The PypeTask.finalized() is intended to be overriden by subclasses. 
    >>> wf.refreshTargets( objs = [fout] )
    test/testfile_in
    test/testfile_out
    test/testfile_in
    test/testfile_out
    in finalize: TaskDone
    True
    >>> #The following code show how to set up a task with a PypeThreadWorkflow that allows running multitple tasks in parallel. 
    >>> from PypeController import PypeThreadWorkflow
    >>> wf = PypeThreadWorkflow()
    >>> @PypeTask(outputDataObjs={"test_out":fout},
    ...           inputDataObjs={"test_in":fin},
    ...           TaskType=PypeThreadTaskBase,
    ...           parameters={"a":'I am "a"'}, **{"b":'I am "b"'})
    ... def test(self):
    ...     print test.test_in.localFileName
    ...     print test.test_out.localFileName
    ...     os.system( "touch %s" % fn(test.test_out) )
    ...     print self.test_in.localFileName
    ...     print self.test_out.localFileName
    >>> wf.addTask(test)
    >>> def finalize(self):
    ...     def f():
    ...         print "in finalize:", self._status
    ...     return f
    >>> test.finalize = finalize(test)  # For testing only. Please don't do this in your code. The PypeTask.finalized() is intended to be overided by subclasses. 
    >>> wf.refreshTargets( objs = [fout] ) #doctest: +SKIP
    """

    def f(taskFun):
        TaskType = kwargv.get("TaskType", PypeTaskBase)
        if "TaskType" in kwargv:
            del kwargv["TaskType"]

        kwargv["_taskFun"] = taskFun

        if kwargv.get("URL",None) == None:
            kwargv["URL"] = "task://" + inspect.getfile(taskFun) + "/"+ taskFun.func_name
        kwargv["_codeMD5digest"] = hashlib.md5(inspect.getsource(taskFun)).hexdigest()
        kwargv["_paramMD5digest"] = hashlib.md5(repr(kwargv)).hexdigest()

        return TaskType(*argv, **kwargv) 

    return f

def PypeShellTask(*argv, **kwargv):

    """
    A function that converts a shell script into a PypeTaskBase object.

    >>> import os 
    >>> from PypeData import PypeLocalFile, makePypeLocalFile, fn
    >>> from PypeTask import *
    >>> fin = makePypeLocalFile("test/testfile_in", readOnly=False)
    >>> fout = makePypeLocalFile("test/testfile_out", readOnly=False)
    >>> f = open("test/shellTask.sh","w")
    >>> f.write( "echo touch %s; touch %s" % (fn(fout), fn(fout)) )
    >>> f.close()
    >>> shellTask = PypeShellTask(outputDataObjs={"test_out":fout},
    ...                           inputDataObjs={"test_in":fin},
    ...                           parameters={"a":'I am "a"'}, **{"b":'I am "b"'}) 
    >>> shellTask = shellTask("test/shellTask.sh")
    >>> type(shellTask) 
    <class 'PypeTask.PypeTaskBase'>
    >>> print fn(shellTask.test_in)
    test/testfile_in
    >>> os.system( "touch %s" %  fn(fin)  ) 
    0
    >>> timeStampCompare(shellTask.inputDataObjs, shellTask.outputDataObjs, shellTask.parameters)
    True
    >>> print shellTask._getRunFlag()
    True
    >>> shellTask()
    >>> timeStampCompare(shellTask.inputDataObjs, shellTask.outputDataObjs, shellTask.parameters)
    False
    >>> print shellTask._getRunFlag()
    False
    >>> shellTask()
    """

    def f(scriptToRun):
        def taskFun():
            """make shell script using a template"""
            """run shell command"""
            shellCmd = "/bin/bash %s" % scriptToRun
            runShellCmd(shlex.split(shellCmd))

        kwargv["script"] = scriptToRun
        return PypeTask(*argv, **kwargv)(taskFun)

    return f


def PypeSGETask(*argv, **kwargv):

    """
    Similar to PypeShellTask, but the shell script job will be executed through SGE.
    """

    def f(scriptToRun):

        def taskFun():
            """make shell script using the template"""
            """run shell command"""
            shellCmd = "qsub -sync y -S /bin/bash %s" % scriptToRun
            runShellCmd(shlex.split(shellCmd))

        kwargv["script"] = scriptToRun

        return PypeTask(*argv, **kwargv)(taskFun)

    return f

def PypeDistributibleTask(*argv, **kwargv):

    """
    Similar to PypeShellTask and PypeSGETask, with an additional argument "distributed" to decide
    whether a job to be run through local shell or SGE.
    """

    distributed = kwargv.get("distributed", False)
    def f(scriptToRun):
        def taskFun(self):
            """make shell script using the template"""
            """run shell command"""
            if distributed == True:
                shellCmd = "qsub -sync y -S /bin/bash %s" % scriptToRun
            else:
                shellCmd = "/bin/bash %s" % scriptToRun

            runShellCmd(shlex.split(shellCmd))

        kwargv["script"] = scriptToRun
        return PypeTask(*argv, **kwargv)(taskFun) 

    return f

def timeStampCompare( inputDataObjs, outputDataObjs, parameters) :

    """
    Given the inputDataObjs and the outputDataObjs, determine whether any
    object in the inputDataObjs is created or modified later than any object
    in outputDataObjects.
    """

    runFlag = False

    inputDataObjsTS = []
    for ft, f in inputDataObjs.iteritems():
        inputDataObjsTS.append( f.timeStamp )

    outputDataObjsTS = []

    for ft, f in outputDataObjs.iteritems():
        if not f.exists:
            runFlag = True
            break
        else:
            outputDataObjsTS.append( f.timeStamp )

    if runFlag == False:                
        if min(outputDataObjsTS) < max(inputDataObjsTS):
            runFlag = True


    return runFlag

if __name__ == "__main__":
    import doctest
    doctest.testmod()
