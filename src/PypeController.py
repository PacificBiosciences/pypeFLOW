import threading
from threading import Thread
import inspect
import hashlib
import os
import time
from Queue import Queue
from cStringIO import StringIO
from urlparse import urlparse

from rdflib.Graph import ConjunctiveGraph as Graph
from rdflib import Namespace
from rdflib import Literal
from rdflib import URIRef
from rdflib.TripleStore import TripleStore

from PypeCommon import pypeNS, URLSchemeNotSupportYet, runShellCmd, PypeObject
from PypeTask import PypeTask, PypeThreadTaskBase, PypeTaskBase

class PypeNode(object):
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

    def __init__(self, RDFGraph, subGraphNodes=None):
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
            
    def tSort(self, connectePypeNode = None): #return a topoloical sort node list
        edges = self._allEdges.copy()
        nodes = self._allNodes.copy()
        
        S = [x for x in self._allNodes if x.inDegree == 0]
        L = []
        #print("len(S):",len(S))
        while len(S) != 0:
            n = S.pop()
            L.append(n)
            outNodes = n._outNodes.copy()
            for m in outNodes:
                #print("1:",len(edges))
                edges.remove( (n, m) )
                n.removeAnOutNode(m)
                m.removeAnInNode(n)
                #print("2:",len(edges))
                if m.inDegree == 0:
                    S.append(m)
            #print ("len(edges):",len(edges))
            #print ("len(S):", len(S))
        
        if len(edges) != 0:
            print( "circle detected" )
            return None
        else:
            return [x.obj for x in L]
                    

class PypeWorkflow(PypeObject):

    supportedURLScheme = ["workflow"]

    def __init__(self, URL = None,  **attributes ):

        if URL == None:
            URL = "workflow://pype/./" + __file__+"/%d" % id(self)

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
            self.addObjects(taskObj.inputFiles.values())
            self.addObjects(taskObj.outputFiles.values())
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

    @property 
    def graphvizDot(self):
        graph = self._RDFGraph
        dotStr = StringIO()
        shapeMap = {"file":"box", "task":"component"}
        dotStr.write( 'digraph "%s" {\n' % self.URL)
        for URL in self._pypeObjects.keys():
            URLParseResult = urlparse(URL)
            if URLParseResult.scheme not in shapeMap:
                continue
            else:
                shape = shapeMap[URLParseResult.scheme]          
                dotStr.write( '"%s" [shape=%s];\n' % (URL, shape))
                
        for row in graph.query('SELECT ?s ?o WHERE {?s pype:prereq ?o . }', initNs=dict(pype=pypeNS)):
            s, o = row
            #s = urlparse(s).path.split("/")[-1]
            #o = urlparse(o).path.split("/")[-1]
            dotStr.write( '"%s" -> "%s";\n' % (o, s))
        dotStr.write ("}")
        return dotStr.getvalue()
     
    def refreshTargets(self, objs = []):
        if len(objs) != 0:
            connectedPypeNodes = set()
            for obj in objs:
                for x in self._RDFGraph.transitive_objects(URIRef(obj.URL), pypeNS["prereq"]):
                    connectedPypeNodes.add(x)
            tSortedURLs = PypeGraph(self._RDFGraph, connectedPypeNodes).tSort(connectedPypeNodes)
        else:
            tSortedURLs = PypeGraph(self._RDFGraph).tSort(connectedPypeNodes)

        for URL in tSortedURLs:
            obj = self._pypeObjects[URL]
            if not isinstance(obj, PypeTaskBase):
                continue
            else:
                obj()
            
    @property
    def RDFXML(self):
        return self._RDFGraph.serialize() 

class PypeThreadWorklow(PypeWorkflow):
    CONCURRENT_THREAD_ALLOWED = 4

    #@classmethod
    #def setNumThreadAllowed(cls,nT)
    #    cls.CONCURRENT_THREAD_ALLOWED = nT

    def refreshTargets(self, objs = []):
        if len(objs) != 0:
            connectedPypeNodes = set()
            for obj in objs:
                for x in self._RDFGraph.transitive_objects(URIRef(obj.URL), pypeNS["prereq"]):
                    connectedPypeNodes.add(x)
            tSortedURLs = PypeGraph(self._RDFGraph, connectedPypeNodes).tSort(connectedPypeNodes)
        else:
            tSortedURLs = PypeGraph(self._RDFGraph).tSort(connectedPypeNodes)

        sortedTaskList = [ (str(u), self._pypeObjects[u], None) for u in tSortedURLs if isinstance(self._pypeObjects[u], PypeTaskBase) ]
        jobStatusMap = dict( ( (t[0], t[2]) for t in sortedTaskList ) ) 
        prereqJobURLMap = {}
        for URL, taskObj, tStatus in sortedTaskList:
            prereqJobURLs = [str(u) for u in self._RDFGraph.transitive_objects(URIRef(URL), pypeNS["prereq"]) 
                                    if isinstance(self._pypeObjects[str(u)], PypeTaskBase) and str(u) != URL ]
            prereqJobURLMap[URL] = prereqJobURLs

        nSubmittedJob = 0

        while 1:
            jobsReadyToBeSubmitted = []
            for URL, taskObj, tStatus in sortedTaskList:
                prereqJobURLs = prereqJobURLMap[URL]
                if len(prereqJobURLs) == 0 and jobStatusMap[URL] == None:
                    jobsReadyToBeSubmitted.append( (URL, taskObj) )
                elif all( ( jobStatusMap[u] in ["done", "continue"] for u in prereqJobURLs ) ) and jobStatusMap[URL] == None:
                    jobsReadyToBeSubmitted.append( (URL, taskObj) )


            for URL, taskObj in jobsReadyToBeSubmitted:
                if nSubmittedJob < PypeThreadWorklow.CONCURRENT_THREAD_ALLOWED:
                    t = Thread(target = taskObj)
                    t.start()
                    nSubmittedJob += 1
                else:
                    break


            #t = Thread(target=taskObj)
            #t.start()
            #t.join()
            time.sleep(0.5)
            print
            print
            print "number of running tasks", threading.active_count()-1
            while not self.messageQueue.empty(): 
                URL, message = self.messageQueue.get()
                jobStatusMap[str(URL)] = message

                if message in ["done", "continue"]:
                    nSubmittedJob -= 1
            for u,s in jobStatusMap.items():
                print u, s

            if threading.active_count() == 1 and len(jobsReadyToBeSubmitted) == 0:
                break

def test():

    from PypeData import PypeLocalFile, makePypeLocalFile

    wf = PypeWorkflow()

    f1 = makePypeLocalFile("test.fa")
    f2 = makePypeLocalFile("ref.fa")
    f3 = makePypeLocalFile("aln.txt", readOnly=False)
    f4 = makePypeLocalFile("aln2.txt", readOnly=False)

    os.system("touch %s" % f1.localFileName)
    os.system("touch %s" % f2.localFileName)
    
    @PypeTask(inputFiles={"fasta":f1, "ref":f2},
              outputFiles={"aln":f3},
              parameters={"a":10}, **{"b":12})
    def testTask(*argv, **kwargv):
        print("testTask is running")
        for ft, f in testTask.outputFiles.iteritems():
            #os.system("touch %s" % f.localFileName)
            runShellCmd(["touch", "%s" % f.localFileName])
            runShellCmd(["sleep", "5" ])

    @PypeTask(inputFiles={"fasta":f1, "aln":f3},
              outputFiles={"aln2":f4},
              parameters={"a":10}, **{"b":12})
    def testTask2(*argv, **kwargv):
        print("testTask2 is running")
        for ft, f in testTask2.outputFiles.iteritems():
            #os.system("touch %s" % f.localFileName)
            runShellCmd(["touch", "%s" % f.localFileName])
        
    #wf.addObjects([f1,f2,f3,f4])
    #wf.addObjects([testTask, testTask2])
    
    wf.addTasks([testTask, testTask2])

    print (wf.RDFXML)
    print (wf.graphvizDot)

    #aGraph = PypeGraph(wf._RDFGraph)
    #print(aGraph.tSort())

    wf.refreshTargets([f4])

    print("re-touch f1")
    os.system("sleep 1;touch %s;" % f1.localFileName)
    wf.refreshTargets([f4])

    print("re-touch f3")
    os.system("sleep 1;touch %s;" % f3.localFileName)
    wf.refreshTargets([f4])

    print("re-touch f1, target=f3")
    os.system("sleep 1;touch %s;" % f1.localFileName)
    wf.refreshTargets([f3])

    
def test4Threading():

    from PypeData import PypeLocalFile, makePypeLocalFile

    mq = Queue()
    wf = PypeThreadWorklow(messageQueue=mq)
    allTasks = []
    for i in range(10):
        f1 = makePypeLocalFile("test%02d_in" % i )
        f2 = makePypeLocalFile("test%02d_out" % i)
        os.system("touch %s" % f1.localFileName)

        def f(self):
            #self._queue.put( self.infile.localFileName)
            #self._queue.put( self.outfile.localFileName)
            runShellCmd(["sleep", "2" ])

        task = PypeTask(inputFiles={"infile":f1},
                        outputFiles={"outfile":f2},
                        URL="task://pype/./task%d" %i,
                        TaskType=PypeThreadTaskBase) ( f )

        task.setMessageQueue(mq)
         
        wf.addObjects([f1,f2])
        wf.addTask(task)
        allTasks.append(task)

    wf.refreshTargets(allTasks)

if __name__ == "__main__":
    #test()
    test4Threading()
