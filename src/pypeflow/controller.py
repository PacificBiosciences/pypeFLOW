import datetime

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

PypeController: This module provides the PypeWorkflow that controlls how a workflow is excuted.

"""


import threading 
import time 
import logging
from threading import Thread
from Queue import Queue
from multiprocessing import Process 
from multiprocessing import Queue as MPQueue
from multiprocessing import Event as MPEvent
from cStringIO import StringIO 
from urlparse import urlparse

from common import * 
from data import PypeDataObjectBase, PypeSplittableLocalFile
from task import *

logger = logging.getLogger(__name__)

class TaskExecutionError(PypeError):
    pass

class TaskTypeError(PypeError):
    pass

class PypeNode(object):

    """ 
    Representing a node in the dependence DAG. 
    """

    def __init__(self, obj):
        self.obj = obj
        self._outNodes = set()
        self._inNodes = set()

    def addAnOutNode(self, obj):
        self._outNodes.add(obj)
        
    def addAnInNode(self, obj):
        self._inNodes.add(obj)

    def removeAnOutNode(self, obj):
        self._outNodes.remove(obj)

    def removeAnInNode(self, obj):
        self._inNodes.remove(obj)

    @property
    def inDegree(self):
        return len(self._inNodes)

    @property
    def outDegree(self):
        return len(self._outNodes)
    
    @property
    def depth(self):
        if self.inDegree == 0:
            return 1
        return 1 + max([ node.depth for node in self._inNodes ])

class PypeGraph(object):

    """ 
    Representing a dependence DAG with PypeObjects. 
    """

    def __init__(self, RDFGraph, subGraphNodes=None):
        
        """
        Construct an internal DAG with PypeObject given an RDF graph.
        A sub-graph can be constructed if subGraphNodes is not "None"
        """

        self._RDFGraph = RDFGraph
        self._allEdges = set()
        self._allNodes = set()
        self.url2Node ={}

        for row in self._RDFGraph.query('SELECT ?s ?o WHERE {?s pype:prereq ?o . }', initNs=dict(pype=pypeNS)):
            if subGraphNodes != None:
                if row[0] not in subGraphNodes: continue
                if row[1] not in subGraphNodes: continue
            
            sURL, oURL = str(row[0]), str(row[1])
            
            self.url2Node[sURL] = self.url2Node.get( sURL, PypeNode(str(row[0])) )
            self.url2Node[oURL] = self.url2Node.get( oURL, PypeNode(str(row[1])) )

            n1 = self.url2Node[oURL]
            n2 = self.url2Node[sURL]
            
            n1.addAnOutNode(n2)
            n2.addAnInNode(n1)
            
            anEdge = (n1, n2)
            self._allNodes.add( n1 )
            self._allNodes.add( n2 )
            self._allEdges.add( anEdge )
            
    def __getitem__(self, url):
        """PypeGraph["URL"] ==> PypeNode"""
        return self.url2Node[url]

    def tSort(self): #return a topoloical sort node list
        
        """
        Output topological sorted list of the graph element. 
        It raises a TeskExecutionError if a circle is detected.
        """

        edges = self._allEdges.copy()
        
        S = [x for x in self._allNodes if x.inDegree == 0]
        L = []
        while len(S) != 0:
            n = S.pop()
            L.append(n)
            outNodes = n._outNodes.copy()
            for m in outNodes:
                edges.remove( (n, m) )
                n.removeAnOutNode(m)
                m.removeAnInNode(n)
                if m.inDegree == 0:
                    S.append(m)
        
        if len(edges) != 0:
            raise TaskExecutionError(" Circle detectd in the dependency graph ")
        else:
            return [x.obj for x in L]
                    
class PypeWorkflow(PypeObject):

    """ 
    Representing a PypeWorkflow. PypeTask and PypeDataObjects can be added
    into the workflow and executed through the instanct methods.

    >>> import os, time 
    >>> from pypeflow.data import PypeLocalFile, makePypeLocalFile, fn
    >>> from pypeflow.task import *
    >>> try:
    ...     os.makedirs("/tmp/pypetest")
    ...     os.system("rm /tmp/pypetest/*")
    ... except:
    ...     pass
    >>> time.sleep(1)
    >>> fin = makePypeLocalFile("/tmp/pypetest/testfile_in", readOnly=False)
    >>> fout = makePypeLocalFile("/tmp/pypetest/testfile_out", readOnly=False)
    >>> @PypeTask(outputDataObjs={"test_out":fout},
    ...           inputDataObjs={"test_in":fin},
    ...           parameters={"a":'I am "a"'}, **{"b":'I am "b"'})
    ... def test(self):
    ...     print test.test_in.localFileName
    ...     print test.test_out.localFileName
    ...     os.system( "touch %s" % fn(test.test_out) )
    ...     pass
    >>> os.system( "touch %s" %  (fn(fin))  )
    0
    >>> from pypeflow.controller import PypeWorkflow
    >>> wf = PypeWorkflow()
    >>> wf.addTask(test)
    >>> def finalize(self):
    ...     def f():
    ...         print "in finalize:", self._status
    ...     return f
    >>> test.finalize = finalize(test)  # For testing only. Please don't do this in your code. The PypeTask.finalized() is intended to be overriden by subclasses. 
    >>> wf.refreshTargets( objs = [fout] )
    /tmp/pypetest/testfile_in
    /tmp/pypetest/testfile_out
    in finalize: TaskDone
    True
    """

    supportedURLScheme = ["workflow"]

    def __init__(self, URL = None, **attributes ):

        if URL == None:
            URL = "workflow://" + __file__+"/%d" % id(self)

        self._pypeObjects = {}

        PypeObject.__init__(self, URL, **attributes)

        self._referenceRDFGraph = None #place holder for a reference RDF

        
    def addObject(self, obj):
        self.addObjects([obj])

    def addObjects(self, objs):
        """
        Add data objects into the workflow. One can add also task object to the workflow using this method for
        non-threaded workflow.
        """
        for obj in objs:
            if obj.URL in self._pypeObjects:
                if id(self._pypeObjects[obj.URL]) != id(obj):
                    raise PypeError, "Add different objects with the same URL %s" % obj.URL
                else:
                    continue
            self._pypeObjects[obj.URL] = obj

    def addTask(self, taskObj):
        self.addTasks([taskObj])


    def addTasks(self, taskObjs):

        """
        Add tasks into the workflow. The dependent input and output data objects are added automatically too. 
        It sets the message queue used for communicating between the task thread and the main thread. One has
        to use addTasks() or addTask() to add task objects to a threaded workflow.
        """

        for taskObj in taskObjs:
            if isinstance(taskObj, PypeTaskCollection):
                for subTaskObj in taskObj.getTasks() + taskObj.getScatterGatherTasks():
                    self.addObjects(subTaskObj.inputDataObjs.values())
                    self.addObjects(subTaskObj.outputDataObjs.values())
                    self.addObjects(subTaskObj.mutableDataObjs.values())
                    self.addObject(subTaskObj)

            else:
                for dObj in taskObj.inputDataObjs.values() +\
                            taskObj.outputDataObjs.values() +\
                            taskObj.mutableDataObjs.values() :
                    if isinstance(dObj, PypeSplittableLocalFile):
                        self.addObjects([dObj._completeFile])
                    self.addObjects([dObj])

                self.addObject(taskObj)

            
    def removeTask(self, taskObj):
        self.removeTasks([taskObj])
        
    def removeTasks(self, taskObjs ):
        """
        Remove tasks from the workflow.
        """
        self.removeObjects(taskObjs)
            
    def removeObjects(self, objs):
        """
        Remove objects from the workflow. If the object cannot be found, a PypeError is raised.
        """
        for obj in objs:
            if obj.URL in self._pypeObjects:
                del self._pypeObjects[obj.URL]
            else:
                raise PypeError, "Unable to remove %s from the graph. (Object not found)" % obj.URL

    def updateURL(self, oldURL, newURL):
        obj = self._pypeObjects[oldURL]
        obj._updateURL(newURL)
        self._pypeObjects[newURL] = obj
        del self._pypeObjects[oldURL]


            
    @property
    def _RDFGraph(self):
        graph = Graph()
        for URL, obj in self._pypeObjects.iteritems():
            for s,p,o in obj._RDFGraph:
                graph.add( (s,p,o) )
        return graph

    def setReferenceRDFGraph(self, fn):
        self._referenceRDFGraph = Graph()
        self._referenceRDFGraph.load(fn)
        refMD5s = self._referenceRDFGraph.subject_objects(pypeNS["codeMD5digest"])
        for URL, md5digest in refMD5s:
            obj = self._pypeObjects[str(URL)]
            obj.setReferenceMD5(md5digest)

    def _graphvizDot(self, shortName=False):
        graph = self._RDFGraph
        dotStr = StringIO()
        shapeMap = {"file":"box", "state":"box", "task":"component"}
        colorMap = {"file":"yellow", "state":"cyan", "task":"green"}
        dotStr.write( 'digraph "%s" {\n rankdir=LR;' % self.URL)
        for URL in self._pypeObjects.keys():
            URLParseResult = urlparse(URL)
            if URLParseResult.scheme not in shapeMap:
                continue
            else:
                shape = shapeMap[URLParseResult.scheme]
                color = colorMap[URLParseResult.scheme]

                s = URL
                if shortName == True:
                    s = URLParseResult.scheme + "://..." + URLParseResult.path.split("/")[-1] 
                dotStr.write( '"%s" [shape=%s, fillcolor=%s, style=filled];\n' % (s, shape, color))

        for row in graph.query('SELECT ?s ?o WHERE {?s pype:prereq ?o . }', initNs=dict(pype=pypeNS)):
            s, o = row
            if shortName == True:
                    s = urlparse(s).scheme + "://..." + urlparse(s).path.split("/")[-1] 
                    o = urlparse(o).scheme + "://..." + urlparse(o).path.split("/")[-1] 
            dotStr.write( '"%s" -> "%s";\n' % (o, s))
        for row in graph.query('SELECT ?s ?o WHERE {?s pype:hasMutable ?o . }', initNs=dict(pype=pypeNS)):
            s, o = row
            if shortName == True:
                    s = urlparse(s).scheme + "://..." + urlparse(s).path.split("/")[-1] 
                    o = urlparse(o).scheme + "://..." + urlparse(o).path.split("/")[-1] 
            dotStr.write( '"%s" -- "%s" [arrowhead=both, style=dashed ];\n' % (s, o))
        dotStr.write ("}")
        return dotStr.getvalue()

    @property
    def graphvizDot(self):
        return self._graphvizDot()

    @property
    def graphvizShortNameDot(self):
        return self._graphvizDot(shortName = True)

    @property
    def makeFileStr(self):
        
        """
        generate a string that has the information of the execution dependency in
        a "Makefile" like format. It can be written into a "Makefile" and
        executed by "make".
        """

        for URL in self._pypeObjects.keys():
            URLParseResult = urlparse(URL)
            if URLParseResult.scheme != "task": continue
            taskObj = self._pypeObjects[URL]
            if not hasattr(taskObj, "script"):
                raise TaskTypeError("can not convert non shell script based workflow to a makefile") 

        makeStr = StringIO()
        for URL in self._pypeObjects.keys():
            URLParseResult = urlparse(URL)
            if URLParseResult.scheme != "task": continue
            taskObj = self._pypeObjects[URL]
            inputFiles = taskObj.inputDataObjs
            outputFiles = taskObj.outputDataObjs
            #for oStr in [o.localFileName for o in outputFiles.values()]:
            if 1:
                oStr = " ".join( [o.localFileName for o in outputFiles.values()])

                iStr = " ".join([i.localFileName for i in inputFiles.values()])
                makeStr.write( "%s:%s\n" % ( oStr, iStr ) )
                makeStr.write( "\t%s\n\n" % taskObj.script )
        makeStr.write("all: %s" %  " ".join([o.localFileName for o in outputFiles.values()]) )
        return makeStr.getvalue()


    def refreshTargets(self, objs = [], callback = (None, None, None) ):

        """
        Execute the DAG to reach all objects in the "objs" argument.
        """

        if len(objs) != 0:
            connectedPypeNodes = set()
            for obj in objs:
                if isinstance(obj, PypeSplittableLocalFile):
                    obj = obj._completeFile
                for x in self._RDFGraph.transitive_objects(URIRef(obj.URL), pypeNS["prereq"]):
                    connectedPypeNodes.add(x)
            tSortedURLs = PypeGraph(self._RDFGraph, connectedPypeNodes).tSort()
        else:
            tSortedURLs = PypeGraph(self._RDFGraph).tSort( )

        for URL in tSortedURLs:
            obj = self._pypeObjects[URL]
            if not isinstance(obj, PypeTaskBase):
                continue
            else:
                obj()
                obj.finalize()

        self._runCallback(callback)

        return True

    def _runCallback(self, callback = (None, None, None ) ):

        if callback[0] != None and callable(callback[0]):
            argv = []
            kwargv = {}
            if callback[1] != None and isinstance( callback[1], type(list()) ):
                argv = callback[1]
            else:
                raise TaskExecutionError( "callback argument type error") 

            if callback[2] != None and isinstance( callback[1], type(dict()) ):
                kwargv = callback[2]
            else:
                raise TaskExecutionError( "callback argument type error") 

            callback[0](*argv, **kwargv)

        elif callback[0] != None:
            raise TaskExecutionError( "callback is not callable") 
    
    @property
    def dataObjects( self ):
        return [ o for o in self._pypeObjects.values( ) if isinstance( o, PypeDataObjectBase )]
    
    @property
    def tasks( self ):
        return [ o for o in self._pypeObjects.values( ) if isinstance( o, PypeTaskBase )]

    @property
    def inputDataObjects(self):
        graph = self._RDFGraph
        inputObjs = []
        for obj in self.dataObjects:
            r = graph.query('SELECT ?o WHERE {<%s> pype:prereq ?o .  }' % obj.URL, initNs=dict(pype=pypeNS))
            if len(r) == 0:
                inputObjs.append(obj)
        return inputObjs
     
    @property
    def outputDataObjects(self):
        graph = self._RDFGraph
        outputObjs = []
        for obj in self.dataObjects:
            r = graph.query('SELECT ?s WHERE {?s pype:prereq <%s> .  }' % obj.URL, initNs=dict(pype=pypeNS))
            if len(r) == 0:
                outputObjs.append(obj)
        return outputObjs

class PypeThreadWorkflow(PypeWorkflow):

    """ 
    Representing a PypeWorkflow that can excute tasks concurrently using threads. It
    assume all tasks block until they finish. PypeTask and PypeDataObjects can be added
    into the workflow and executed through the instanct methods.
    """

    CONCURRENT_THREAD_ALLOWED = 16
    MAX_NUMBER_TASK_SLOT = CONCURRENT_THREAD_ALLOWED

    @classmethod
    def setNumThreadAllowed(cls, nT, nS):
        """
        Override the defualt number of threads used to run the tasks with this method.
        """
        cls.CONCURRENT_THREAD_ALLOWED = nT
        cls.MAX_NUMBER_TASK_SLOT = nS

    def __init__(self, URL = None, **attributes ):
        PypeWorkflow.__init__(self, URL, **attributes )
        if self.__class__ == PypeThreadWorkflow:
            self.messageQueue = Queue()
            self.shutdown_event = threading.Event()
        if self.__class__ == PypeMPWorkflow:
            self.messageQueue = MPQueue()
            self.shutdown_event = MPEvent()
        self.jobStatusMap = {}

    def addTasks(self, taskObjs):

        """
        Add tasks into the workflow. The dependent input and output data objects are added automatically too. 
        It sets the message queue used for communicating between the task thread and the main thread. One has
        to use addTasks() or addTask() to add task objects to a threaded workflow.
        """

        for taskObj in taskObjs:
            if isinstance(taskObj, PypeTaskCollection):
                for subTaskObj in taskObj.getTasks() + taskObj.getScatterGatherTasks():
                    if not isinstance(subTaskObj, PypeThreadTaskBase):
                        raise TaskTypeError("Only PypeThreadTask can be added into a PypeThreadWorkflow. The task object %s has type %s " % (subTaskObj.URL, repr(type(subTaskObj))))
                    subTaskObj.setMessageQueue(self.messageQueue)
                    subTaskObj.setShutdownEvent(self.shutdown_event)
            else:
                if not isinstance(taskObj, PypeThreadTaskBase):
                    raise TaskTypeError("Only PypeThreadTask can be added into a PypeThreadWorkflow. The task object has type %s " % repr(type(subTaskObj)))
                taskObj.setMessageQueue(self.messageQueue)
                taskObj.setShutdownEvent(self.shutdown_event)

        PypeWorkflow.addTasks(self, taskObjs)

    def refreshTargets( self, objs = [], 
                              callback = (None, None, None), 
                              updateFreq = None, 
                              exitOnFailure = True ):
        try:
            rtn = self._refreshTargets(objs = objs, callback = callback, updateFreq = updateFreq, exitOnFailure = exitOnFailure)
        except (KeyboardInterrupt, SystemExit) as e:
            logger.debug( "SIGINT, trying to kill the working threads. Threaded task function should use 'self.shutdown_event' to catch the signal and stop properly.")
            print
            print "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
            print "! Please wait for all threads / processes to terminate !"
            print "! Also, maybe use 'ps' or 'qstat' to check all threads,!"
            print "! processes and/or jobs are terminated cleanly.        !"
            print "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
            self.shutdown_event.set()
            raise e
        return rtn


    def _refreshTargets( self, objs = [], 
                               callback = (None, None, None), 
                               updateFreq = None, 
                               exitOnFailure =True ):

        if self.__class__ == PypeThreadWorkflow:
            thread = Thread
        if self.__class__ == PypeMPWorkflow:
            thread = Process

        rdfGraph = self._RDFGraph # expensive to recompute, should not change during execution
        if len(objs) != 0:
            connectedPypeNodes = set()
            for obj in objs:
                if isinstance(obj, PypeSplittableLocalFile):
                    obj = obj._completeFile
                for x in rdfGraph.transitive_objects(URIRef(obj.URL), pypeNS["prereq"]):
                    connectedPypeNodes.add(x)
            tSortedURLs = PypeGraph(rdfGraph, connectedPypeNodes).tSort( )
        else:
            tSortedURLs = PypeGraph(rdfGraph).tSort( )

        sortedTaskList = [ (str(u), self._pypeObjects[u], None) for u in tSortedURLs 
                            if isinstance(self._pypeObjects[u], PypeTaskBase) ]

        self.jobStatusMap = dict( ( (t[0], t[2]) for t in sortedTaskList ) )

        prereqJobURLMap = {}

        for URL, taskObj, tStatus in sortedTaskList:
            prereqJobURLs = [str(u) for u in rdfGraph.transitive_objects(URIRef(URL), pypeNS["prereq"])
                                    if isinstance(self._pypeObjects[str(u)], PypeTaskBase) and str(u) != URL ]

            prereqJobURLMap[URL] = prereqJobURLs

            logger.debug("Determined prereqs for %s to be %s" % (URL, ", ".join(prereqJobURLs)))
            
            if taskObj.nSlots > self.MAX_NUMBER_TASK_SLOT:
                raise TaskExecutionError("%s requests more %s task slots which is more than %d task slots allowed" %
                                          (str(URL), taskObj.nSlots, self.MAX_NUMBER_TASK_SLOT) )  

        nSubmittedJob = 0
        usedTaskSlots = 0
        loopN = 0
        task2thread = {}
        lastUpdate = None
        activeDataObjs = set() #keep a set of output and mutable data object. a task can not be submitted if a running task has the same output

        while 1:

            loopN += 1
            logger.info( "tick: %d" % loopN )
            jobsReadyToBeSubmitted = []

            for URL, taskObj, tStatus in sortedTaskList:
                prereqJobURLs = prereqJobURLMap[URL]
                outputCollision = False

                for dataObj in taskObj.outputDataObjs.values() + taskObj.mutableDataObjs.values():
                    for fromTaskObjURL, activeDataObjURL in activeDataObjs:
                        if dataObj.URL == activeDataObjURL and taskObj.URL != fromTaskObjURL:
                            logger.debug( "output collision detected for data object:"+str(dataObj))
                            outputCollision = True
                            break
                
                if outputCollision:
                    continue

                if (len(prereqJobURLs) == 0 and self.jobStatusMap[URL] == None) or\
                   (all( ( self.jobStatusMap[u] in ["done", "continue"] for u in prereqJobURLs ) ) and self.jobStatusMap[URL] == None):

                    jobsReadyToBeSubmitted.append( (URL, taskObj) )

                    for dataObj in taskObj.outputDataObjs.values() + taskObj.mutableDataObjs.values():
                        logger.debug( "add active data obj:"+str(dataObj))
                        activeDataObjs.add( (taskObj.URL, dataObj.URL) )

            logger.info( "jobReadyToBeSubmitted: %s" % len(jobsReadyToBeSubmitted) )

            numAliveThreads = len( [ t for t in task2thread.values() if t.is_alive() ] )
            #better job status detection, messageQueue should be empty and all returen condition should be "done", "continue" or "fail"
            if numAliveThreads == 0 and len(jobsReadyToBeSubmitted) == 0 and self.messageQueue.empty(): 
                logger.info( "_refreshTargets() finished with no thread running and no new job to submit" )
                for URL in task2thread:
                    assert self.jobStatusMap[str(URL)] in ("done", "continue", "fail") 

                break

            for URL, taskObj in jobsReadyToBeSubmitted:
                numberOfEmptySlot = self.MAX_NUMBER_TASK_SLOT - usedTaskSlots 
                logger.info( "number of empty slot = %d/%d" % (numberOfEmptySlot, self.MAX_NUMBER_TASK_SLOT))
                if numberOfEmptySlot >= taskObj.nSlots and numAliveThreads < self.CONCURRENT_THREAD_ALLOWED:
                    t = thread(target = taskObj)
                    t.start()
                    task2thread[URL] = t
                    nSubmittedJob += 1
                    usedTaskSlots += taskObj.nSlots
                    numAliveThreads += 1
                    self.jobStatusMap[URL] = "submitted"
                else:
                    break

            time.sleep(1)
            if updateFreq != None:
                elapsedSeconds = updateFreq if lastUpdate==None else (datetime.datetime.now()-lastUpdate).seconds
                if elapsedSeconds >= updateFreq:
                    self._update( elapsedSeconds )
                    lastUpdate = datetime.datetime.now( )

            logger.info ( "number of running tasks: %d" % (threading.activeCount()-1) )

            failedJobCount = 0

            while not self.messageQueue.empty():

                URL, message = self.messageQueue.get()
                self.jobStatusMap[str(URL)] = message

                if message in ["done", "continue"]:
                    successfullTask = self._pypeObjects[str(URL)]
                    nSubmittedJob -= 1
                    usedTaskSlots -= successfullTask.nSlots
                    task2thread[URL].join()
                    successfullTask.finalize()

                    for o in successfullTask.outputDataObjs.values() + successfullTask.mutableDataObjs.values():
                        activeDataObjs.remove( (successfullTask.URL, o.URL) )

                elif message in ["fail"]:
                    failedTask = self._pypeObjects[str(URL)]
                    nSubmittedJob -= 1
                    usedTaskSlots -= failedTask.nSlots
                    task2thread[URL].join()
                    failedJobCount += 1
                    failedTask.finalize()

                    for o in failedTask.outputDataObjs.values() + failedTask.mutableDataObjs.values():
                        activeDataObjs.remove( (failedTask.URL, o.URL) )

            for u,s in sorted(self.jobStatusMap.items()):
                logger.info( "task status: %s, %s, used slots: %d" % (str(u),str(s), self._pypeObjects[str(u)].nSlots) )

            if failedJobCount != 0 and exitOnFailure:
                for url, thread in task2thread.iteritems( ):
                    if thread.isAlive( ):
                        thread.join( )
                        self._pypeObjects[str(url)].finalize()
                return False


        for u,s in sorted(self.jobStatusMap.items()):
            logger.info( "task status: %s, %s" % (str(u),str(s)) )

        self._runCallback(callback)
        return True
    
    def _update(self, elapsed):
        """Can be overridden to provide timed updates during execution"""
        pass

    def _graphvizDot(self, shortName=False):

        graph = self._RDFGraph
        dotStr = StringIO()
        shapeMap = {"file":"box", "state":"box", "task":"component"}
        colorMap = {"file":"yellow", "state":"cyan", "task":"green"}
        dotStr.write( 'digraph "%s" {\n rankdir=LR;' % self.URL)


        for URL in self._pypeObjects.keys():
            URLParseResult = urlparse(URL)
            if URLParseResult.scheme not in shapeMap:
                continue
            else:
                shape = shapeMap[URLParseResult.scheme]
                color = colorMap[URLParseResult.scheme]

                s = URL
                if shortName == True:
                    s = URLParseResult.scheme + "://..." + URLParseResult.path.split("/")[-1] 

                if URLParseResult.scheme == "task":
                    jobStatus = self.jobStatusMap.get(URL, None)
                    if jobStatus != None:
                        if jobStatus == "fail":
                            color = 'red'
                        elif jobStatus == "done":
                            color = 'green'
                    else:
                        color = 'white'
                    
                dotStr.write( '"%s" [shape=%s, fillcolor=%s, style=filled];\n' % (s, shape, color))

        for row in graph.query('SELECT ?s ?o WHERE {?s pype:prereq ?o . }', initNs=dict(pype=pypeNS)):
            s, o = row
            if shortName == True:
                s = urlparse(s).scheme + "://..." + urlparse(s).path.split("/")[-1] 
                o = urlparse(o).scheme + "://..." + urlparse(o).path.split("/")[-1] 
            dotStr.write( '"%s" -> "%s";\n' % (o, s))
        for row in graph.query('SELECT ?s ?o WHERE {?s pype:hasMutable ?o . }', initNs=dict(pype=pypeNS)):
            s, o = row
            if shortName == True:
                    s = urlparse(s).scheme + "://..." + urlparse(s).path.split("/")[-1] 
                    o = urlparse(o).scheme + "://..." + urlparse(o).path.split("/")[-1] 
            dotStr.write( '"%s" -- "%s" [arrowhead=both, style=dashed ];\n' % (s, o))
        dotStr.write ("}")
        return dotStr.getvalue()

class PypeMPWorkflow(PypeThreadWorkflow):
    """ Just for a name space to indicate the workflow using multiprocessing.
    """
    pass


def defaultOutputTemplate(fn):
    return fn + ".out"

def applyFOFN( task_fun = None, 
               fofonFileName = None, 
               outTemplateFunc = defaultOutputTemplate,
               nproc = 8 ):
               
    tasks = getFOFNMapTasks( FOFNFileName = fofonFileName, 
                             outTemplateFunc = outTemplateFunc, 
                             TaskType=PypeThreadTaskBase,
                             parameters = dict(nSlots = 1))( task_fun )

    wf = PypeThreadWorkflow()
    wf.CONCURRENT_THREAD_ALLOWED = nproc 
    wf.MAX_NUMBER_TASK_SLOT = nproc
    wf.addTasks(tasks)
    wf.refreshTargets(exitOnFailure=False)


if __name__ == "__main__":
    import doctest
    doctest.testmod()
