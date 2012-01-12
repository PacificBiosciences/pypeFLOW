
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

import sys
import os 


from pypeflow.common import * 
from pypeflow.task import PypeTask, PypeShellTask, PypeSGETask, PypeThreadTaskBase, PypeTaskBase, PypeDistributibleTask
from pypeflow.controller import PypeWorkflow, PypeThreadWorkflow
from pypeflow.data import PypeLocalFile, makePypeLocalFile
from Queue import Queue 
import logging

logger = logging.getLogger('workflow')
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
ch.setFormatter(formatter)
logger.addHandler(ch)


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
    logger.info("test start")
    baseDir = "."
    import random
    random.seed(1984)
    PypeThreadWorkflow.setNumThreadAllowed(20,20)
    wf = PypeThreadWorkflow()
    allTasks = []
    for layer in range(5):
        fN = random.randint(3,7)
        fin = [None] * fN
        fout = [None] * fN
        for w in range(fN):
            fin[w] = makePypeLocalFile(baseDir + "/testdata/testfile_l%d_w%d.dat" % (layer, w) )
            fout[w] = makePypeLocalFile(baseDir + "/testdata/testfile_l%d_w%d.dat" % (layer+1, w) )
            wf.addObjects([fin[w], fout[w]])

        for w in range(fN):
            inputDataObjs = {}
            outputDataObjs = {}
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
                    runShellCmd(["sleep", "%d" % random.randint(0,20) ])

                    for of in self.outputDataObjs.values():
                        runShellCmd(["touch", of.localFileName])

                task = PypeTask(inputDataObjs = inputDataObjs,
                                outputDataObjs = outputDataObjs, 
                                URL="task://task_l%d_w%d" % (layer, w), 
                                TaskType=PypeThreadTaskBase) ( t1 )

            elif runmode == "localshell":
                task = PypeShellTask(inputDataObjs = inputDataObjs,
                                     outputDataObjs = outputDataObjs, 
                                     URL="task://task_l%d_w%d" % (layer, w), 
                                     TaskType=PypeThreadTaskBase) ( "%s" % shellFileName )

            elif runmode == "sge": 
                task = PypeSGETask(inputDataObjs = inputDataObjs,
                                   outputDataObjs = outputDataObjs, 
                                   URL="task://task_l%d_w%d" % (layer, w), 
                                   TaskType=PypeThreadTaskBase) ( "%s" % shellFileName )

            elif runmode == "mixed":
                #distributed = random.choice( (False, True) )
                distributed = True if w % 3 == 0 else False
                task = PypeDistributibleTask(inputDataObjs = inputDataObjs,
                                   outputDataObjs = outputDataObjs,
                                   URL="task://task_l%d_w%d" % (layer, w), 
                                   distributed=distributed,
                                   TaskType=PypeThreadTaskBase) ( "%s" % shellFileName )

            wf.addTasks([task])
            allTasks.append(task)

    for URL in wf._pypeObjects:
        prereqJobURLs = [str(u) for u in wf._RDFGraph.transitive_objects(URIRef(URL), pypeNS["prereq"])
                                        if isinstance(wf._pypeObjects[str(u)], PypeLocalFile) and str(u) != URL ]
        if len(prereqJobURLs) == 0:
            if cleanup == "1":
                os.system("echo start > %s" % wf._pypeObjects[URL].localFileName)
            pass
    wf.refreshTargets(allTasks)
    dotFile = open("test.dot","w")
    print >>dotFile, wf.graphvizShortNameDot
    dotFile.close()
    rdfFile = open("test.rdf","w")
    print >>rdfFile, wf.RDFXML
    rdfFile.close()
    mkFile = open("test.mk","w")
    print >>mkFile, wf.makeFileStr
    mkFile.close()

if __name__ == "__main__":
    try:
        testDistributed(sys.argv[1], sys.argv[2])
    except IndexError:
        print "try: python PypeTest.py localshell 1"
        print "running simpleTest()"
        simpleTest()

