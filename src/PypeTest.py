import sys
import os 

from PypeCommon import * 
from PypeTask import PypeTask, PypeShellTask, PypeSGETask, PypeThreadTaskBase, PypeTaskBase, PypeDistributibleTask
from PypeController import PypeWorkflow, PypeThreadWorkflow
from PypeData import PypeLocalFile, makePypeLocalFile
from Queue import Queue 

def simpleTest():


    wf = PypeWorkflow()

    f1 = makePypeLocalFile("test.fa")
    f2 = makePypeLocalFile("ref.fa")
    f3 = makePypeLocalFile("aln.txt", readOnly=False)
    f4 = makePypeLocalFile("aln2.txt", readOnly=False)

    os.system("touch %s" % f1.localFileName)
    os.system("touch %s" % f2.localFileName)
    
    @PypeTask(inputDataObjs={"fasta":f1, "ref":f2},
              outputDataObjs={"aln":f3},
              parameters={"a":10}, **{"b":12})
    def testTask(*argv, **kwargv):
        print("testTask is running")
        for ft, f in testTask.outputDataObjs.iteritems():
            #os.system("touch %s" % f.localFileName)
            runShellCmd(["touch", "%s" % f.localFileName])
            runShellCmd(["sleep", "5" ])

    @PypeTask(inputDataObjs={"fasta":f1, "aln":f3},
              outputDataObjs={"aln2":f4},
              parameters={"a":10}, **{"b":12})
    def testTask2(*argv, **kwargv):
        print("testTask2 is running")
        for ft, f in testTask2.outputDataObjs.iteritems():
            #os.system("touch %s" % f.localFileName)
            runShellCmd(["touch", "%s" % f.localFileName])
        
    #wf.addObjects([f1,f2,f3,f4]) wf.addObjects([testTask, testTask2])
    
    wf.addTasks([testTask, testTask2])

    print (wf.RDFXML)
    print (wf.graphvizDot)

    #aGraph = PypeGraph(wf._RDFGraph) print(aGraph.tSort())

    wf.refreshTargets([f4])

    print("re-touch f1")
    os.system("sleep 1;touch %s;" % f1.localFileName)
    wf.refreshTargets([f4])

    print("re-touch f3")
    os.system("sleep 1;touch %s;" % f3.localFileName)

def testDistributed(runmode, cleanup):
    baseDir = "/home/UNIXHOME/jchin/task2011/PypeEngineIntegrationTest/src"
    #baseDir = "/home/cschin/Sandbox/PypeEngine/src"
    import random
    random.seed(1984)
    mq = Queue()
    PypeThreadWorkflow.setNumThreadAllowed(20)
    #wf = PypeWorkflow(messageQueue=mq)
    wf = PypeThreadWorkflow(messageQueue=mq)
    allTasks = []
    for layer in range(5):
        #fN = random.randint(3,7)
        fN = 5
        fin = [None] * fN
        fout = [None] * fN
        for w in range(fN):
            fin[w] = makePypeLocalFile(baseDir + "/testdata/testfile_l%d_w%d.dat" % (layer, w) )
            fout[w] = makePypeLocalFile(baseDir + "/testdata/testfile_l%d_w%d.dat" % (layer+1, w) )
            wf.addObjects([fin[w], fout[w]])

        for w in range(fN):
            inputDataObjs = {}
            outputDataObjs = {}
            #for i in range(random.randint(1,5)):
            for i in range(5):
                inputDataObjs["infile%d" % i] = random.choice(fin)

            outputDataObjs["outfile%d" % w] = fout[w] 


            shellCmd = "sleep 1\n" + "\n".join([ "echo %d %d ...  > %s" % (layer, w, of.localFileName) for of in outputDataObjs.values() ]) + "\nsleep 10"
            shellFileName = baseDir + "/testdata/task_l%d_w%d.sh" % (layer, w)
            shfile = open(shellFileName, 'w')
            print >> shfile, shellCmd
            shfile.close()

            if runmode == "internal":
                def t1(self):
                    #self._queue.put( self.infile.localFileName) 
                    #self._queue.put( self.outfile.localFileName)
                    runShellCmd(["sleep", "%d" % random.randint(0,20) ])

                    for of in self.outputDataObjs.values():
                        runShellCmd(["touch", of.localFileName])

                task = PypeTask(inputDataObjs = inputDataObjs,
                                outputDataObjs = outputDataObjs, 
                                URL="task://pype/./task_l%d_w%d" % (layer, w), 
                                TaskType=PypeThreadTaskBase) ( t1 )
                task.setMessageQueue(mq)

            elif runmode == "localshell":
                task = PypeShellTask(inputDataObjs = inputDataObjs,
                                     outputDataObjs = outputDataObjs, 
                                     URL="task://pype/./task_l%d_w%d" % (layer, w), 
                                     TaskType=PypeThreadTaskBase) ( "%s" % shellFileName )
                task.setMessageQueue(mq)

            elif runmode == "sge": 
                task = PypeSGETask(inputDataObjs = inputDataObjs,
                                   outputDataObjs = outputDataObjs, 
                                   URL="task://pype/task_l%d_w%d" % (layer, w), 
                                   TaskType=PypeThreadTaskBase) ( "%s" % shellFileName )
                task.setMessageQueue(mq)

            elif runmode == "mixed":
                #distributed = random.choice( (False, True) )
                distributed = True if w % 3 == 0 else False
                task = PypeDistributibleTask(inputDataObjs = inputDataObjs,
                                   outputDataObjs = outputDataObjs,
                                   URL="task://pype/./task_l%d_w%d" % (layer, w), 
                                   distributed=distributed,
                                   TaskType=PypeThreadTaskBase) ( "%s" % shellFileName )
                task.setMessageQueue(mq)
                


            wf.addTasks([task])
            allTasks.append(task)

    for URL in wf._pypeObjects:
        prereqJobURLs = [str(u) for u in wf._RDFGraph.transitive_objects(URIRef(URL), pypeNS["prereq"])
                                        if isinstance(wf._pypeObjects[str(u)], PypeLocalFile) and str(u) != URL ]
        if len(prereqJobURLs) == 0:
            if cleanup == "1":
                os.system("echo start > %s" % wf._pypeObjects[URL].localFileName)
            pass
    dotFile = open("test.dot","w")
    print >>dotFile, wf.graphvizDot
    dotFile.close()
    rdfFile = open("test.rdf","w")
    print >>rdfFile, wf.RDFXML
    rdfFile.close()
    wf.refreshTargets(allTasks)

if __name__ == "__main__":
    testDistributed(sys.argv[1], sys.argv[2])

