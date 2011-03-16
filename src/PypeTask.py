from urlparse import urlparse
import inspect
import hashlib
import json
import os

from rdflib.Graph import ConjunctiveGraph as Graph
from rdflib import Namespace
from rdflib import Literal
from rdflib import URIRef
from PypeCommon import pypeNS, URLSchemeNotSupportYet



class TaskFunctionError(Exception):
    def __init__(self, msg):
        self.msg = msg
    def __str__(self):
        return repr(self.msg)

class PypeTaskBase(object):

    supportedURLScheme = ["task"]

    def __init__(self, *argv, **kwargv):

        self._argv = argv
        self._kwargv = kwargv
        self._taskFun = kwargv['_taskFun']

        self.URL = None
        self._RDFGraph = None
        self._referenceMD5 = None

        taskURL = kwargv.get("taskURL", None)

        if taskURL != None:
            URLParseResult = urlparse(taskURL)
            if URLParseResult.scheme not in PypeTask.supportedURLScheme:
                raise URLSchemeNotSupportYet("%s schema is not supported yet for PypeTask")
            else:
                self.URL = taskURL
            
        self.__dict__.update(kwargv)
                    
        for defaultAttr in ["inputFiles", "outputFiles", "parameters"]:
            if defaultAttr not in self.__dict__:
                self.__dict__[defaultAttr] = {}
            vars(self).update(self.__dict__[defaultAttr])

        self._codeMD5digest = kwargv["_codeMD5digest"]
        #print func.func_name, self._codeMD5digest
        self._paramMD5digest = kwargv["_paramMD5digest"]

        self._updateRDFGraph()

    def setReferenceMD5(self, md5Str):
        self._referenceMD5 = md5Str

    def _getRunFlag(self):
        runFlag = False

        inputFiles = self.inputFiles
        outputFiles = self.outputFiles
        parameters = self.parameters

        inputFilesTS = []
        for ft, f in inputFiles.iteritems():
            if not os.path.exists(f.localFileName):
                raise TaskFunctionError("Input file does not exist: task = %s, input file= %s" % (self.URL, f.URL))
            inputFilesTS.append( os.stat(f.localFileName).st_mtime )
   
        outputFilesTS = []
        for ft, f in outputFiles.iteritems():
            if not os.path.exists(f.localFileName):
                runFlag = True
                break
            else:
                outputFilesTS.append( os.stat(f.localFileName).st_mtime )

        if runFlag == False:                
            if min(outputFilesTS) < max(inputFilesTS):
                runFlag = True

        #print self._referenceMD5
        #print self._codeMD5digest
        if self._referenceMD5 != None and self._referenceMD5 != self._codeMD5digest:
            self._referenceMD5 = self._codeMD5digest
            runFlag = True

        return runFlag

    def _runTask(self, *argv, **kwargv):
        argspec = inspect.getargspec(self._taskFun)
        #print argspec
        if argspec.keywords != None:
            self._taskFun(*argv, **kwargv)
        elif argspec.varargs != None:
            self._taskFun(*argv)
        elif len(argspec.args) != 0:
            nkwarg = {}
            defaultArg = argspec.args[-len(argspec.defaults):]
            for a in defaultArg:
                nkwarg[a] = kwargv[a]
            self._taskFun(*argv, **nkwarg)
        else:
            self._taskFun()

    def _updateRDFGraph(self):
        graph = self._RDFGraph = Graph()
        for k,v in self.__dict__.iteritems():
            if k == "URL": continue
            if k[0] == "_": continue
            if k in ["inputFiles", "outputFiles", "parameters"]:
                if k == "inputFiles":
                    for ft, f in v.iteritems():
                        graph.add( (URIRef(self.URL), pypeNS["prereq"], URIRef(f.URL) ) )
                elif k == "outputFiles":
                    for ft, f in v.iteritems():
                        graph.add( (URIRef(f.URL), pypeNS["prereq"], URIRef(self.URL) ) )
                elif k == "parameters":
                    graph.add( (URIRef(self.URL), pypeNS["hasParameters"], Literal(json.dumps(v)) ) )
            
                continue

            if hasattr(v, "URL"):
                graph.add( ( URIRef(self.URL), pypeNS[k], URIRef(v.URL) ) )
            else:
                graph.add( ( URIRef(self.URL), pypeNS[k], Literal(json.dumps(v.__repr__())) ) )

            graph.add(  ( URIRef(self.URL), pypeNS["codeMD5digest"], Literal(self._codeMD5digest) ) )
            graph.add(  ( URIRef(self.URL), pypeNS["parameterMD5digest"], Literal(self._paramMD5digest) ) )
    
    @property
    def RDFXML(self):
        return self._RDFGraph.serialize()                       

    def __call__(self, *argv, **kwargv):
        #print "__call__", argv
        #print "__call__", kwargv
        argv = list(argv)
        argv.extend(self._argv)
        kwargv.update(self._kwargv)

        inputFiles = self.inputFiles
        outputFiles = self.outputFiles
        parameters = self.parameters

        runFlag = self._getRunFlag()
            
        if runFlag == True:
            #self._taskFun(*argv, **kwargv)

            self._runTask(*argv, **kwargv)

            if self.inputFiles != inputFiles or self.parameters != parameters:
                raise TaskFunctionError("The 'inputFiles' and 'parameters' should not be modified in %s" % self.URL)

            self._updateRDFGraph() #allow the task function to modify the output list if necessary

class PypeTaskBase2(PypeTaskBase):
    pass

def PypeTask(*argv, **kwargv):

    def f(taskFun):
        TaskType = kwargv.get("TaskType", PypeTaskBase)
        if "TaskType" in kwargv:
            del kwargv["TaskType"]

        kwargv["_taskFun"] = taskFun

        if kwargv.get("URL",None) == None:
            kwargv["URL"] = "task://pype/./" + inspect.getfile(taskFun) + "/"+ taskFun.func_name
        kwargv["_codeMD5digest"] = hashlib.md5(inspect.getsource(taskFun)).hexdigest()
        #print func.func_name, self._codeMD5digest
        kwargv["_paramMD5digest"] = hashlib.md5(repr(kwargv)).hexdigest()

        return TaskType(*argv, **kwargv) 

    return f


def test():
    from PypeFile import PypeFile, makeLocalPypeFile, fn
    f1 = makeLocalPypeFile("test.fa")
    f2 = makeLocalPypeFile("ref.fa")
    f3 = makeLocalPypeFile("aln.txt", readOnly=False)
    os.system('touch test.fa')
    os.system('touch ref.fa')

    @PypeTask(inputFiles={"fasta":f1, "ref":f2},
              outputFiles={"aln":f3},
              parameters={"a":10}, **{"b":12})
    #def test2(bb, a=2, **kwargv):
    def test2():
        this = test2
        print this.fasta
        print test2.ref.localFileName
        #print argv
        #print kwargv
        #print kwargv["inputFiles"] 
        print "test2 is running"
        #print test2.a, kwargv['a']
        print test2.b
        #print a
        #print "bb",bb
        print test2.__dict__

    print test2.RDFXML
    test2()    
    
if __name__ == "__main__":
    test()
