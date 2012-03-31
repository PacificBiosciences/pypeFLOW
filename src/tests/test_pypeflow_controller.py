from nose import SkipTest
from nose.tools import assert_equal
import pypeflow.task
import pypeflow.data
import pypeflow.controller

class TestPypeNode:
    def test___init__(self):
        # pype_node = PypeNode(obj)
        raise SkipTest # TODO: implement your test here

    def test_addAnInNode(self):
        # pype_node = PypeNode(obj)
        # assert_equal(expected, pype_node.addAnInNode(obj))
        raise SkipTest # TODO: implement your test here

    def test_addAnOutNode(self):
        # pype_node = PypeNode(obj)
        # assert_equal(expected, pype_node.addAnOutNode(obj))
        raise SkipTest # TODO: implement your test here

    def test_depth(self):
        # pype_node = PypeNode(obj)
        # assert_equal(expected, pype_node.depth())
        raise SkipTest # TODO: implement your test here

    def test_inDegree(self):
        # pype_node = PypeNode(obj)
        # assert_equal(expected, pype_node.inDegree())
        raise SkipTest # TODO: implement your test here

    def test_outDegree(self):
        # pype_node = PypeNode(obj)
        # assert_equal(expected, pype_node.outDegree())
        raise SkipTest # TODO: implement your test here

    def test_removeAnInNode(self):
        # pype_node = PypeNode(obj)
        # assert_equal(expected, pype_node.removeAnInNode(obj))
        raise SkipTest # TODO: implement your test here

    def test_removeAnOutNode(self):
        # pype_node = PypeNode(obj)
        # assert_equal(expected, pype_node.removeAnOutNode(obj))
        raise SkipTest # TODO: implement your test here

class TestPypeGraph:
    def test___getitem__(self):
        # pype_graph = PypeGraph(RDFGraph, subGraphNodes)
        # assert_equal(expected, pype_graph.__getitem__(url))
        raise SkipTest # TODO: implement your test here

    def test___init__(self):
        # pype_graph = PypeGraph(RDFGraph, subGraphNodes)
        raise SkipTest # TODO: implement your test here

    def test_tSort(self):
        # pype_graph = PypeGraph(RDFGraph, subGraphNodes)
        # assert_equal(expected, pype_graph.tSort())
        raise SkipTest # TODO: implement your test here

class TestPypeWorkflow:
    def test___init__(self):
        # pype_workflow = PypeWorkflow(URL, **attributes)
        raise SkipTest # TODO: implement your test here

    def test_addObject(self):
        # pype_workflow = PypeWorkflow(URL, **attributes)
        # assert_equal(expected, pype_workflow.addObject(obj))
        raise SkipTest # TODO: implement your test here

    def test_addObjects(self):
        # pype_workflow = PypeWorkflow(URL, **attributes)
        # assert_equal(expected, pype_workflow.addObjects(objs))
        raise SkipTest # TODO: implement your test here

    def test_addTask(self):
        # pype_workflow = PypeWorkflow(URL, **attributes)
        # assert_equal(expected, pype_workflow.addTask(taskObj))
        raise SkipTest # TODO: implement your test here

    def test_addTasks(self):
        # pype_workflow = PypeWorkflow(URL, **attributes)
        # assert_equal(expected, pype_workflow.addTasks(taskObjs))
        raise SkipTest # TODO: implement your test here

    def test_dataObjects(self):
        # pype_workflow = PypeWorkflow(URL, **attributes)
        # assert_equal(expected, pype_workflow.dataObjects())
        raise SkipTest # TODO: implement your test here

    def test_graphvizDot(self):
        # pype_workflow = PypeWorkflow(URL, **attributes)
        # assert_equal(expected, pype_workflow.graphvizDot())
        raise SkipTest # TODO: implement your test here

    def test_graphvizShortNameDot(self):
        # pype_workflow = PypeWorkflow(URL, **attributes)
        # assert_equal(expected, pype_workflow.graphvizShortNameDot())
        raise SkipTest # TODO: implement your test here

    def test_makeFileStr(self):
        # pype_workflow = PypeWorkflow(URL, **attributes)
        # assert_equal(expected, pype_workflow.makeFileStr())
        raise SkipTest # TODO: implement your test here

    def test_refreshTargets(self):
        # pype_workflow = PypeWorkflow(URL, **attributes)
        # assert_equal(expected, pype_workflow.refreshTargets(objs, callback))
        raise SkipTest # TODO: implement your test here

    def test_removeObjects(self):
        # pype_workflow = PypeWorkflow(URL, **attributes)
        # assert_equal(expected, pype_workflow.removeObjects(objs))
        raise SkipTest # TODO: implement your test here

    def test_removeTask(self):
        # pype_workflow = PypeWorkflow(URL, **attributes)
        # assert_equal(expected, pype_workflow.removeTask(taskObj))
        raise SkipTest # TODO: implement your test here

    def test_removeTasks(self):
        # pype_workflow = PypeWorkflow(URL, **attributes)
        # assert_equal(expected, pype_workflow.removeTasks(taskObjs))
        raise SkipTest # TODO: implement your test here

    def test_setReferenceRDFGraph(self):
        # pype_workflow = PypeWorkflow(URL, **attributes)
        # assert_equal(expected, pype_workflow.setReferenceRDFGraph(fn))
        raise SkipTest # TODO: implement your test here

    def test_tasks(self):
        # pype_workflow = PypeWorkflow(URL, **attributes)
        # assert_equal(expected, pype_workflow.tasks())
        raise SkipTest # TODO: implement your test here

    def test_scatterTask(self):
        
        import os
        os.system("rm -rf /tmp/pypetest/*")
        nChunk = 3 

        infileObj0 =\
        pypeflow.data.PypeLocalFile(
                      "file://localhost/tmp/pypetest/test_in_0.txt")
        with open(infileObj0.localFileName,"w") as f:
            f.write("prefix4:")

        infileObj4 =\
        pypeflow.data.PypeSplittableLocalFile(
                      "splittablefile://localhost/tmp/pypetest/test_in_4.txt", 
                      nChunk = nChunk)

        with open(infileObj4.localFileName, "w") as f:
            for i in range(nChunk):
                f.write("file%02d\n" % i)

        def scatter(*argv, **kwargv):
            outputObjs = sorted( kwargv["outputDataObjs"].items() )
            nOut = len(outputObjs)
            outputObjs = [ (o[0], o[1], open(o[1].localFileName, "w")) for o in outputObjs]
            with open(kwargv["inputDataObjs"]["completeFile"].localFileName,"r") as f:
                i = 0
                for l in f:
                    outf = outputObjs[i % nOut][2]
                    outf.write(l)
                    i += 1
            for o in outputObjs:
                o[2].close()

        PypeShellTask = pypeflow.task.PypeShellTask
        PypeTask = pypeflow.task.PypeTask
        PypeTaskBase = pypeflow.task.PypeTaskBase
        infileObj4.setScatterTask(PypeTask, PypeTaskBase, scatter)
        
        def gather(*argv, **kwargv):
            inputObjs = sorted( kwargv["inputDataObjs"].items() )
            with open(kwargv["outputDataObjs"]["completeFile"].localFileName,"w") as outf:
                for k, subfile in inputObjs:
                    f = open(subfile.localFileName)
                    outf.write(f.read())
                    f.close()

        outfileObj4 =\
        pypeflow.data.PypeSplittableLocalFile(
                      "splittablefile://localhost/tmp/pypetest/test_out_4.txt", 
                      nChunk = nChunk)

        outfileObj4.setGatherTask(PypeTask, PypeTaskBase, gather)

        PypeScatteredTasks = pypeflow.task.PypeScatteredTasks

        @PypeScatteredTasks( inputDataObjs = {"inf":infileObj4, "prefix":infileObj0},
                             outputDataObjs = {"outf":outfileObj4},
                             URL="tasks://test_fun_4")
        def test_fun_4(*argv, **kwargv):
            chunk_id = kwargv["chunk_id"]
            self = test_fun_4[chunk_id]

            assert self.inf._path == "/tmp/pypetest/%03d_test_in_4.txt" % chunk_id
            with open( self.prefix.localFileName, "r") as f:
                prefix = f.read()

            with open( self.outf._path, "w") as f:
                in_f = open(self.inf.localFileName,"r")
                f.write(prefix + in_f.read())
                in_f.close()
            return self.inf._path

        outfileObj5 =\
        pypeflow.data.PypeSplittableLocalFile(
                      "splittablefile://localhost/tmp/pypetest/test_out_5.txt", 
                      nChunk = nChunk)
        outfileObj5.setGatherTask(PypeTask, PypeTaskBase, gather)

        @PypeScatteredTasks( inputDataObjs = {"inf":infileObj4, "prefix":infileObj0},
                             outputDataObjs = {"outf":outfileObj5},
                             URL="tasks://test_fun_5")
        def test_fun_5(*argv, **kwargv):
            chunk_id = kwargv["chunk_id"]
            self = test_fun_5[chunk_id]

            assert self.inf._path == "/tmp/pypetest/%03d_test_in_4.txt" % chunk_id
            with open( self.prefix.localFileName, "r") as f:
                prefix = f.read()

            with open( self.outf._path, "w") as f:
                in_f = open(self.inf.localFileName,"r")
                f.write(prefix +"2:"+ in_f.read())
                in_f.close()
            return self.inf._path
        assert len(test_fun_4.getTasks()) == nChunk 

        wf = pypeflow.controller.PypeWorkflow()
        wf.addTasks( [test_fun_4, test_fun_5] )
        print wf.graphvizDot
        wf.refreshTargets( [outfileObj4, outfileObj5] )
    
class TestPypeThreadWorkflow:
    def test___init__(self):
        # pype_thread_workflow = PypeThreadWorkflow(URL, **attributes)
        raise SkipTest # TODO: implement your test here

    def test_addTasks(self):
        # pype_thread_workflow = PypeThreadWorkflow(URL, **attributes)
        # assert_equal(expected, pype_thread_workflow.addTasks(taskObjs))
        raise SkipTest # TODO: implement your test here

    def test_refreshTargets(self):
        # pype_thread_workflow = PypeThreadWorkflow(URL, **attributes)
        # assert_equal(expected, pype_thread_workflow.refreshTargets(objs, callback, updateFreq, exitOnFailure))
        raise SkipTest # TODO: implement your test here

    def test_setNumThreadAllowed(self):
        # pype_thread_workflow = PypeThreadWorkflow(URL, **attributes)
        # assert_equal(expected, pype_thread_workflow.setNumThreadAllowed(nT, nS))
        raise SkipTest # TODO: implement your test here

    def test_mutableDataObjects(self):

        infileObj =\
        pypeflow.data.PypeLocalFile("file://localhost/tmp/pypetest/test_for_shared_output_in.txt")

        outfileObj =\
        pypeflow.data.PypeLocalFile("file://localhost/tmp/pypetest/test_for_shared_output_out.txt")

        out1 =\
        pypeflow.data.PypeLocalFile("file://localhost/tmp/pypetest/test_for_shared_output_out1.txt")

        out2 =\
        pypeflow.data.PypeLocalFile("file://localhost/tmp/pypetest/test_for_shared_output_out2.txt")

        out3 =\
        pypeflow.data.PypeLocalFile("file://localhost/tmp/pypetest/test_for_shared_output_out3.txt")

        import os
        os.system("rm -rf /tmp/pypetest/*")

        with open(infileObj.localFileName,"w") as f:
            f.write("test")

        PypeThreadWorkflow = pypeflow.controller.PypeThreadWorkflow
        PypeThreadTaskBase = pypeflow.controller.PypeThreadTaskBase
        PypeTask = pypeflow.task.PypeTask
        wf = PypeThreadWorkflow()

        @PypeTask(mutableDataObjs={"out":outfileObj},
                  outputDataObjs={"out1":out1},
                  inputDataObjs={"in":infileObj},
                  TaskType=PypeThreadTaskBase)
        def task1(task):
            with open(task.out.localFileName, "a") as f:
                print >>f, "written by task1"
            with open(task.out1.localFileName, "w") as f:
                print >>f, "written by task1"

        @PypeTask(mutableDataObjs={"out":outfileObj},
                  outputDataObjs={"out2":out2},
                  inputDataObjs={"in":infileObj},
                  TaskType=PypeThreadTaskBase)
        def task2(task):
            with open(task.out.localFileName, "a") as f:
                print >>f, "written by task2"
            with open(task.out2.localFileName, "w") as f:
                print >>f, "written by task2"

        @PypeTask(mutableDataObjs={"out":outfileObj},
                  outputDataObjs={"out3":out3},
                  inputDataObjs={"in":infileObj},
                  TaskType=PypeThreadTaskBase)
        def task3(task):
            with open(task.out.localFileName, "a") as f:
                print >>f, "written by task3"
            with open(task.out3.localFileName, "w") as f:
                print >>f, "written by task3"

        wf = PypeThreadWorkflow()
        wf.addTasks([task1, task2, task3])

        wf.refreshTargets()

        outputSet = set()
        outputSet.add("written by task1")
        outputSet.add("written by task2")
        outputSet.add("written by task3")

        with open(outfileObj.localFileName) as f:
            i = 0
            for l in f:
                l = l.strip()
                assert l in outputSet
                i += 1
            assert i == 3

    def test_stateDataObjects(self):

        infileObj =\
        pypeflow.data.PypeLocalFile("file://localhost/tmp/pypetest/test_for_shared_output_in.txt")

        outfileObj =\
        pypeflow.data.PypeLocalFile("file://localhost/tmp/pypetest/test_for_shared_output_out.txt")

        out1 =\
        pypeflow.data.PypeLocalFile("file://localhost/tmp/pypetest/test_for_shared_output_out1.txt")

        out2 =\
        pypeflow.data.PypeLocalFile("file://localhost/tmp/pypetest/test_for_shared_output_out2.txt")

        out3 =\
        pypeflow.data.PypeLocalFile("file://localhost/tmp/pypetest/test_for_shared_output_out3.txt")

        s1 =\
        pypeflow.data.PypeLocalFile("state://localhost/tmp/pypetest/.state1")

        s2 =\
        pypeflow.data.PypeLocalFile("state://localhost/tmp/pypetest/.state2")

        s3 =\
        pypeflow.data.PypeLocalFile("state://localhost/tmp/pypetest/.state3")

        import os
        import time
        os.system("rm -rf /tmp/pypetest/*")
        time.sleep(2)

        with open(infileObj.localFileName,"w") as f:
            f.write("test")

        PypeThreadWorkflow = pypeflow.controller.PypeThreadWorkflow
        PypeThreadTaskBase = pypeflow.controller.PypeThreadTaskBase
        PypeTask = pypeflow.task.PypeTask
        wf = PypeThreadWorkflow()

        @PypeTask(mutableDataObjs = {"out":outfileObj},
                  outputDataObjs = {"out1":out1, "s1":s1},
                  inputDataObjs = {"in":infileObj},
                  TaskType=PypeThreadTaskBase)
        def task1(task):
            with open(task.out.localFileName, "a") as f:
                print >>f, "written by task1"
            with open(task.s1.localFileName, "w") as f:
                print >>f, "state set"
            with open(task.out1.localFileName, "w") as f:
                print >>f, "written by task1"

        @PypeTask(mutableDataObjs = {"out":outfileObj},
                  outputDataObjs = {"out2":out2, "s2":s2},
                  inputDataObjs = {"in":infileObj, "s1":s1},
                  TaskType=PypeThreadTaskBase)
        def task2(task):
            with open(task.out.localFileName, "a") as f:
                print >>f, "written by task2"
            with open(task.s2.localFileName, "w") as f:
                print >>f, "state set"
            with open(task.out2.localFileName, "w") as f:
                print >>f, "written by task2"

        @PypeTask(mutableDataObjs = {"out":outfileObj},
                  outputDataObjs = {"out3":out3, "s3":s3},
                  inputDataObjs = {"in":infileObj, "s2":s2},
                  TaskType=PypeThreadTaskBase)
        def task3(task):
            with open(task.out.localFileName, "a") as f:
                print >>f, "written by task3"
            with open(task.s3.localFileName, "w") as f:
                print >>f, "state set"
            with open(task.out3.localFileName, "w") as f:
                print >>f, "written by task3"

        wf = PypeThreadWorkflow()
        wf.addTasks([task1, task2, task3])

        wf.refreshTargets()

        with open(outfileObj.localFileName) as f:
            i = 0
            for l in f:
                i += 1
                l = l.strip()
                assert l == "written by task%d" % i
        assert i == 3

    def test_stateDataObjects2(self):

        infileObj =\
        pypeflow.data.PypeLocalFile("file://localhost/tmp/pypetest/test_for_shared_output_in.txt")

        outfileObj =\
        pypeflow.data.PypeLocalFile("file://localhost/tmp/pypetest/test_for_shared_output_out.txt")

        out1 =\
        pypeflow.data.PypeLocalFile("file://localhost/tmp/pypetest/test_for_shared_output_out1.txt")

        out2 =\
        pypeflow.data.PypeLocalFile("file://localhost/tmp/pypetest/test_for_shared_output_out2.txt")

        out3 =\
        pypeflow.data.PypeLocalFile("file://localhost/tmp/pypetest/test_for_shared_output_out3.txt")

        s1 =\
        pypeflow.data.PypeLocalFile("state://localhost/tmp/pypetest/.state1")

        s2 =\
        pypeflow.data.PypeLocalFile("state://localhost/tmp/pypetest/.state2")

        s3 =\
        pypeflow.data.PypeLocalFile("state://localhost/tmp/pypetest/.state3")

        import os
        import time
        os.system("rm -rf /tmp/pypetest/*")
        time.sleep(2)

        with open(infileObj.localFileName,"w") as f:
            f.write("test")

        PypeThreadWorkflow = pypeflow.controller.PypeThreadWorkflow
        PypeThreadTaskBase = pypeflow.controller.PypeThreadTaskBase
        PypeTask = pypeflow.task.PypeTask
        wf = PypeThreadWorkflow()

        @PypeTask( outputDataObjs = {"out1":out1, "s1":s1},
                  inputDataObjs = {"in":infileObj},
                  TaskType=PypeThreadTaskBase)
        def task1(task):
            with open(task.s1.localFileName, "w") as f:
                print >>f, "state set"
            with open(task.out1.localFileName, "w") as f:
                print >>f, "written by task1"

        @PypeTask(outputDataObjs = {"out2":out2, "s2":s2},
                  inputDataObjs = {"in":infileObj, "s1":s1},
                  TaskType=PypeThreadTaskBase)
        def task2(task):
            with open(task.s2.localFileName, "w") as f:
                print >>f, "state set"
            with open(task.out2.localFileName, "w") as f:
                print >>f, "written by task2"

        @PypeTask(outputDataObjs = {"out3":out3, "s3":s3},
                  inputDataObjs = {"in":infileObj, "s2":s2},
                  TaskType=PypeThreadTaskBase)
        def task3(task):
            with open(task.s3.localFileName, "w") as f:
                print >>f, "state set"
            with open(task.out3.localFileName, "w") as f:
                print >>f, "written by task3"

        wf = PypeThreadWorkflow()
        wf.addTasks([task1, task2, task3])

        wf.refreshTargets([s3])

        for i in range(1,4):
            with open("/tmp/pypetest/test_for_shared_output_out%d.txt" % i) as f:
                l = f.read().strip()
                assert l == "written by task%d" % i
