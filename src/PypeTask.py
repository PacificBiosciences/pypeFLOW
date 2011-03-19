
from urlparse import urlparse
import inspect
import hashlib

import sys
PYTHONVERSION = sys.version_info[:2]
if PYTHONVERSION == (2,5):
    import simplejson as json
else:
    import json

import os
import shlex

from PypeCommon import * 

def timeStampCompare( inputDataObjs, outputDataObjs, parameters) :

    runFlag = False

    inputDataObjsTS = []
    for ft, f in inputDataObjs.iteritems():
        inputDataObjsTS.append( f.timeStamp )

    outputDataObjsTS = []

    for ft, f in outputDataObjs.iteritems():
        if not f.isExist:
            runFlag = True
            break
        else:
            outputDataObjsTS.append( f.timeStamp )

    if runFlag == False:                
        if min(outputDataObjsTS) < max(inputDataObjsTS):
            runFlag = True


    return runFlag


class TaskFunctionError(Exception):
    def __init__(self, msg):
        self.msg = msg
    def __str__(self):
        return repr(self.msg)

class PypeTaskBase(PypeObject):

    supportedURLScheme = ["task"]

    def __init__(self, URL, *argv, **kwargv):

        PypeObject.__init__(self, URL, **kwargv)

        self._argv = argv
        self._kwargv = kwargv
        self._taskFun = kwargv['_taskFun']
        self._referenceMD5 = None


        for defaultAttr in ["inputDataObjs", "outputDataObjs", "parameters"]:
            if defaultAttr not in self.__dict__:
                self.__dict__[defaultAttr] = {}
            vars(self).update(self.__dict__[defaultAttr])

        self._codeMD5digest = kwargv["_codeMD5digest"]
        #print func.func_name, self._codeMD5digest
        self._paramMD5digest = kwargv["_paramMD5digest"]
        self._compareFuntions = [ timeStampCompare ]

        self._updateRDFGraph()

    def setReferenceMD5(self, md5Str):
        self._referenceMD5 = md5Str


    def _getRunFlag(self):
        #print self._referenceMD5
        #print self._codeMD5digest
        runFlag = False
        if self._referenceMD5 != None and self._referenceMD5 != self._codeMD5digest:
            self._referenceMD5 = self._codeMD5digest
            runFlag = True

        runFlag = any( [runFlag] + [ f(self.inputDataObjs, self.outputDataObjs, self.parameters) for f in self._compareFuntions] )

        return runFlag

    def _runTask(self, *argv, **kwargv):
        """ TODO: the arg porcessing is still a mess, need to find a better way to do this """
        if PYTHONVERSION == (2,5):
            (args, varargs, varkw, defaults)  = inspect.getargspec(self._taskFun)
            print  (args, varargs, varkw, defaults)
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
                graph.add( ( URIRef(self.URL), pypeNS["inputFile"], URIRef(v.URL) ) )
                continue

            if k in self.outputDataObjs:
                graph.add( ( URIRef(self.URL), pypeNS["outputFile"], URIRef(v.URL) ) )
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

        inputDataObjs = self.inputDataObjs
        outputDataObjs = self.outputDataObjs
        parameters = self.parameters

        runFlag = self._getRunFlag()
            
        if runFlag == True:
            #self._taskFun(*argv, **kwargv)

            self._runTask(*argv, **kwargv)

            if self.inputDataObjs != inputDataObjs or self.parameters != parameters:
                raise TaskFunctionError("The 'inputDataObjs' and 'parameters' should not be modified in %s" % self.URL)

            self._updateRDFGraph() #allow the task function to modify the output list if necessary

class PypeThreadTaskBase(PypeTaskBase):
    def __init__(self, URL, *argv, **kwargv):
        PypeTaskBase.__init__(self, URL, *argv, **kwargv)
    def setMessageQueue(self, q):
        self._queue = q
    def __call__(self, *argv, **kwargv):
        #runFlag = self._getRunFlag()
        try:
            runFlag = self._getRunFlag()
        except TaskFunctionError:
            self._queue.put( (self.URL, "fail") )
            return

        self._queue.put( (self.URL, "started, runflag: %d" % runFlag) )

        PypeTaskBase.__call__(self, *argv, **kwargv)
        self._queue.put( (self.URL, "done") )

class PypeDistributiableTaskBase(PypeThreadTaskBase):

    def __init__(self, URL, *argv, **kwargv):
        PypeTaskBase.__init__(self, URL, *argv, **kwargv)
        self.distributed = True

    def setMessageQueue(self, q):
        self._queue = q

    def __call__(self, *argv, **kwargv):
        #runFlag = self._getRunFlag()
        try:
            runFlag = self._getRunFlag()
        except TaskFunctionError:
            pass
            self._queue.put( (self.URL, "fail") )
            return
        self._queue.put( (self.URL, "started, runflag: %d" % runFlag) )

        PypeTaskBase.__call__(self, *argv, **kwargv)
        self._queue.put( (self.URL, "done") )

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

def PypeShellTask(*argv, **kwargv):

    def f(shellCmd):
        def taskFun():
            """make shell script using the template"""
            """run shell command"""
            runShellCmd(shlex.split(shellCmd))


        TaskType = kwargv.get("TaskType", PypeTaskBase)
        if "TaskType" in kwargv:
            del kwargv["TaskType"]

        kwargv["_taskFun"] = taskFun
        kwargv["shellCmd"] = shellCmd

        if kwargv.get("URL",None) == None:
            kwargv["URL"] = "task://pype/./" + inspect.getfile(taskFun) + "/"+ taskFun.func_name
        kwargv["_codeMD5digest"] = hashlib.md5(inspect.getsource(taskFun)).hexdigest()
        #print func.func_name, self._codeMD5digest
        kwargv["_paramMD5digest"] = hashlib.md5(repr(kwargv)).hexdigest()
                    
        
        return TaskType(*argv, **kwargv) 

    return f


def PypeSGETask(*argv, **kwargv):

    def f(scriptToRun):
        def taskFun():
            """make shell script using the template"""
            """run shell command"""
            shellCmd = "qsub -sync y -S /bin/bash %s" % scriptToRun
            runShellCmd(shlex.split(shellCmd))


        TaskType = kwargv.get("TaskType", PypeTaskBase)
        if "TaskType" in kwargv:
            del kwargv["TaskType"]

        kwargv["_taskFun"] = taskFun
        kwargv["shellCmd"] = scriptToRun

        if kwargv.get("URL",None) == None:
            kwargv["URL"] = "task://pype/./" + inspect.getfile(taskFun) + "/"+ taskFun.func_name
        kwargv["_codeMD5digest"] = hashlib.md5(inspect.getsource(taskFun)).hexdigest()
        #print func.func_name, self._codeMD5digest
        kwargv["_paramMD5digest"] = hashlib.md5(repr(kwargv)).hexdigest()
                    
        
        return TaskType(*argv, **kwargv) 

    return f

def PypeDistributibleTask(*argv, **kwargv):

    def f(scriptToRun):
        def taskFun(self):
            """make shell script using the template"""
            """run shell command"""
            if self.distributed == True:
                shellCmd = "qsub -sync y -S /bin/bash %s" % scriptToRun
            else:
                shellCmd = "/bin/bash %s" % scriptToRun

            runShellCmd(shlex.split(shellCmd))


        TaskType = PypeDistributiableTaskBase
        if "TaskType" in kwargv:
            del kwargv["TaskType"]

        kwargv["_taskFun"] = taskFun
        kwargv["shellCmd"] = scriptToRun

        if kwargv.get("URL",None) == None:
            kwargv["URL"] = "task://pype/./" + inspect.getfile(taskFun) + "/"+ taskFun.func_name
        kwargv["_codeMD5digest"] = hashlib.md5(inspect.getsource(taskFun)).hexdigest()
        #print func.func_name, self._codeMD5digest
        kwargv["_paramMD5digest"] = hashlib.md5(repr(kwargv)).hexdigest()
                    
        
        return TaskType(*argv, **kwargv) 

    return f

def test():
    from PypeData import PypeLocalFile, makePypeLocalFile, fn
    f1 = makePypeLocalFile("test.fa")
    f2 = makePypeLocalFile("ref.fa")
    f3 = makePypeLocalFile("aln.txt", readOnly=False)
    os.system('touch test.fa')
    os.system('touch ref.fa')

    @PypeTask(inputDataObjs={"fasta":f1, "ref":f2},
              outputDataObjs={"aln":f3},
              parameters={"a":10}, **{"b":12})
    #def test2(bb, a=2, **kwargv):
    def test2():
        this = test2
        print this.fasta
        print test2.ref.localFileName
        #print argv
        #print kwargv
        #print kwargv["inputDataObjs"] 
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
