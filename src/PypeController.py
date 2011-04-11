
# @author Jason Chin
#
# Copyright (C) 2010 by Jason Chin 
# Copyright (C) 2011 by Jason Chin
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
from threading import Thread 
import inspect 
import hashlib 
import sys
import os 
import time 
from Queue import Queue 
from cStringIO import StringIO 
from urlparse import urlparse

from PypeCommon import * 
from PypeTask import PypeTask, PypeShellTask, PypeSGETask, PypeThreadTaskBase, PypeTaskBase, PypeDistributibleTask
from PypeData import PypeDataObjectBase

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
        obj2Node ={}

        for row in self._RDFGraph.query('SELECT ?s ?o WHERE {?s pype:prereq ?o . }', initNs=dict(pype=pypeNS)):
            if subGraphNodes != None:
                if row[0] not in subGraphNodes: continue
                if row[1] not in subGraphNodes: continue

            obj2Node[row[0]] = obj2Node.get( row[0], PypeNode(str(row[0])) )
            obj2Node[row[1]] = obj2Node.get( row[1], PypeNode(str(row[1])) )

            n1 = obj2Node[row[1]]
            n2 = obj2Node[row[0]]
            
            n1.addAnOutNode(n2)
            n2.addAnInNode(n1)
            
            anEdge = (n1, n2)
            self._allNodes.add( n1 )
            self._allNodes.add( n2 )
            self._allEdges.add( anEdge )
            
    def tSort(self): #return a topoloical sort node list
        
        """
        Output topological sorted list of the graph element. 
        It raises a TeskExecutionError if a circle is detected.
        """

        edges = self._allEdges.copy()
        nodes = self._allNodes.copy()
        
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
        for obj in objs:
            if obj.URL in self._pypeObjects:
                continue
            self._pypeObjects[obj.URL] = obj
        self._updateRDFGraph()

    def addTask(self, taskObj):
        self.addTasks([taskObj])

    def addTasks(self, taskObjs):
        for taskObj in taskObjs:
            self.addObjects(taskObj.inputDataObjs.values())
            self.addObjects(taskObj.outputDataObjs.values())
            self.addObject(taskObj)

    def _updateRDFGraph(self):
        self._RDFGraph = Graph()
        graph = self._RDFGraph
        for URL, obj in self._pypeObjects.iteritems():
            for s,p,o in obj._RDFGraph:
                graph.add( (s,p,o) )

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
        shapeMap = {"file":"box", "task":"component"}
        colorMap = {"file":"yellow", "task":"green"}
        dotStr.write( 'digraph "%s" {\n' % self.URL)
        for URL in self._pypeObjects.keys():
            URLParseResult = urlparse(URL)
            if URLParseResult.scheme not in shapeMap:
                continue
            else:
                shape = shapeMap[URLParseResult.scheme]
                color = colorMap[URLParseResult.scheme]

                s = URL
                if shortName == True:
                    s = URLParseResult.path.split("/")[-1] 
                dotStr.write( '"%s" [shape=%s, fillcolor=%s, style=filled];\n' % (s, shape, color))

        for row in graph.query('SELECT ?s ?o WHERE {?s pype:prereq ?o . }', initNs=dict(pype=pypeNS)):
            s, o = row
            if shortName == True:
                s = urlparse(s).path.split("/")[-1] 
                o = urlparse(o).path.split("/")[-1]
            dotStr.write( '"%s" -> "%s";\n' % (o, s))
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

        graph = self._RDFGraph
        for URL in self._pypeObjects.keys():
            URLParseResult = urlparse(URL)
            if URLParseResult.scheme != "task": continue
            taskObj = self._pypeObjects[URL]
            if not hasattr(taskObj, "script"):
                raise TaskTypeError("can not convert non shell script based workflow to a makefile") 

        makeStr = StringIO()
        shapeMap = {"file":"box", "task":"component"}
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
     
    def refreshTargets(self, objs = []):

        """
        Execute the DAG to reach all objects in the "objs" argument.
        """

        if len(objs) != 0:
            connectedPypeNodes = set()
            for obj in objs:
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
    
    @property
    def dataObjects( self ):
        return [ o for o in self._pypeObjects.values( ) if isinstance( o, PypeDataObjectBase )]
    
    @property
    def tasks( self ):
        return [ o for o in self._pypeObjects.values( ) if isinstance( o, PypeTaskBase )]

class PypeThreadWorkflow(PypeWorkflow):

    """ 
    Representing a PypeWorkflow that can excute tasks concurrently using threads. It
    assume all tasks block until they finish. PypeTask and PypeDataObjects can be added
    into the workflow and executed through the instanct methods.
    """

    CONCURRENT_THREAD_ALLOWED = 8

    @classmethod
    def setNumThreadAllowed(cls, nT):
        cls.CONCURRENT_THREAD_ALLOWED = nT

    def __init__(self, URL = None, **attributes ):
        PypeWorkflow.__init__(self, URL, **attributes )
        self.messageQueue = Queue()
        self.jobStatusMap = {}

    def addTasks(self, taskObjs):
        for taskObj in taskObjs:
            if not isinstance(taskObj, PypeThreadTaskBase):
                raise TaskTypeError("Only PypeThreadTask can be added into a PypeThreadWorkflow")
            taskObj.setMessageQueue(self.messageQueue)

            self.addObjects(taskObj.inputDataObjs.values())
            self.addObjects(taskObj.outputDataObjs.values())
            self.addObject(taskObj)

    def refreshTargets(self, objs = []):
        if len(objs) != 0:
            connectedPypeNodes = set()
            for obj in objs:
                for x in self._RDFGraph.transitive_objects(URIRef(obj.URL), pypeNS["prereq"]):
                    connectedPypeNodes.add(x)
            tSortedURLs = PypeGraph(self._RDFGraph, connectedPypeNodes).tSort( )
        else:
            tSortedURLs = PypeGraph(self._RDFGraph).tSort( )

        sortedTaskList = [ (str(u), self._pypeObjects[u], None) for u in tSortedURLs if isinstance(self._pypeObjects[u], PypeTaskBase) ]
        self.jobStatusMap = dict( ( (t[0], t[2]) for t in sortedTaskList ) )
        prereqJobURLMap = {}
        for URL, taskObj, tStatus in sortedTaskList:
            prereqJobURLs = [str(u) for u in self._RDFGraph.transitive_objects(URIRef(URL), pypeNS["prereq"])
                                    if isinstance(self._pypeObjects[str(u)], PypeTaskBase) and str(u) != URL ]
            prereqJobURLMap[URL] = prereqJobURLs

        nSubmittedJob = 0
        loopN = 0
        task2thread = {}
        while 1:
            loopN += 1
            print
            print
            print loopN
            jobsReadyToBeSubmitted = []
            for URL, taskObj, tStatus in sortedTaskList:
                prereqJobURLs = prereqJobURLMap[URL]
                if len(prereqJobURLs) == 0 and self.jobStatusMap[URL] == None:
                    jobsReadyToBeSubmitted.append( (URL, taskObj) )
                elif all( ( self.jobStatusMap[u] in ["done", "continue"] for u in prereqJobURLs ) ) and self.jobStatusMap[URL] == None:
                    jobsReadyToBeSubmitted.append( (URL, taskObj) )

            print "jobReadyToBeSubmitted:", len(jobsReadyToBeSubmitted)
            if threading.activeCount() == 1 and len(jobsReadyToBeSubmitted) == 0: #better job status detection, using "running" status rather than checking the thread lib?
                break


            for URL, taskObj in jobsReadyToBeSubmitted:
                if nSubmittedJob < PypeThreadWorkflow.CONCURRENT_THREAD_ALLOWED:
                    t = Thread(target = taskObj)
                    t.start()
                    task2thread[URL] = t
                    nSubmittedJob += 1
                else:
                    break

            time.sleep(0.25)
            print "number of running tasks", threading.activeCount()-1
            faildJobCount = 0

            while not self.messageQueue.empty():
                URL, message = self.messageQueue.get()
                self.jobStatusMap[str(URL)] = message

                if message in ["done", "continue"]:
                    nSubmittedJob -= 1
                    task2thread[URL].join()

                elif message in ["fail"]:
                    failedTask = self._pypeObjects[str(URL)]

                    task2thread[URL].join()
                    faildJobCount += 1

            for u,s in sorted(self.jobStatusMap.items()):
                print u, s

            if faildJobCount != 0:
                break

        print
        for u,s in sorted(self.jobStatusMap.items()):
            print u, s

    def _graphvizDot(self, shortName=False):
        graph = self._RDFGraph
        dotStr = StringIO()
        shapeMap = {"file":"box", "task":"component"}
        colorMap = {"file":"yellow", "task":"green"}
        dotStr.write( 'digraph "%s" {\n' % self.URL)


        for URL in self._pypeObjects.keys():
            URLParseResult = urlparse(URL)
            if URLParseResult.scheme not in shapeMap:
                continue
            else:
                shape = shapeMap[URLParseResult.scheme]
                color = colorMap[URLParseResult.scheme]

                s = URL
                if shortName == True:
                    s = URLParseResult.path.split("/")[-1] 
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
                s = urlparse(s).path.split("/")[-1] 
                o = urlparse(o).path.split("/")[-1]
            dotStr.write( '"%s" -> "%s";\n' % (o, s))
        dotStr.write ("}")
        return dotStr.getvalue()
