from nose.tools import assert_equal
from nose import SkipTest
import pypeflow.task
import pypeflow.data

class TestPypeTaskBase:
    def test___call__(self):
        # pype_task_base = PypeTaskBase(URL, *argv, **kwargv)
        # assert_equal(expected, pype_task_base.__call__(*argv, **kwargv))
        raise SkipTest # TODO: implement your test here

    def test___init__(self):
        # pype_task_base = PypeTaskBase(URL, *argv, **kwargv)
        raise SkipTest # TODO: implement your test here

    def test_finalize(self):
        # pype_task_base = PypeTaskBase(URL, *argv, **kwargv)
        # assert_equal(expected, pype_task_base.finalize())
        raise SkipTest # TODO: implement your test here

    def test_setInputs(self):
        # pype_task_base = PypeTaskBase(URL, *argv, **kwargv)
        # assert_equal(expected, pype_task_base.setInputs(inputDataObjs))
        raise SkipTest # TODO: implement your test here

    def test_setOutputs(self):
        # pype_task_base = PypeTaskBase(URL, *argv, **kwargv)
        # assert_equal(expected, pype_task_base.setOutputs(outputDataObjs))
        raise SkipTest # TODO: implement your test here

    def test_setReferenceMD5(self):
        # pype_task_base = PypeTaskBase(URL, *argv, **kwargv)
        # assert_equal(expected, pype_task_base.setReferenceMD5(md5Str))
        raise SkipTest # TODO: implement your test here

    def test_status(self):
        # pype_task_base = PypeTaskBase(URL, *argv, **kwargv)
        # assert_equal(expected, pype_task_base.status())
        raise SkipTest # TODO: implement your test here

class TestPypeThreadTaskBase:
    def test___call__(self):
        # pype_thread_task_base = PypeThreadTaskBase()
        # assert_equal(expected, pype_thread_task_base.__call__(*argv, **kwargv))
        raise SkipTest # TODO: implement your test here

    def test_nSlots(self):
        # pype_thread_task_base = PypeThreadTaskBase()
        # assert_equal(expected, pype_thread_task_base.nSlots())
        raise SkipTest # TODO: implement your test here

    def test_setMessageQueue(self):
        # pype_thread_task_base = PypeThreadTaskBase()
        # assert_equal(expected, pype_thread_task_base.setMessageQueue(q))
        raise SkipTest # TODO: implement your test here

class TestPypeDistributiableTaskBase:
    def test___init__(self):
        # pype_distributiable_task_base = PypeDistributiableTaskBase(URL, *argv, **kwargv)
        raise SkipTest # TODO: implement your test here


class TestPypeTask:
    def test_pype_task(self):
        # assert_equal(expected, PypeTask(*argv, **kwargv))
        raise SkipTest # TODO: implement your test here

class TestPypeShellTask:
    def test_pype_shell_task(self):
        # assert_equal(expected, PypeShellTask(*argv, **kwargv))
        raise SkipTest # TODO: implement your test here

class TestPypeSGETask:
    def test_pype_sge_task(self):
        # assert_equal(expected, PypeSGETask(*argv, **kwargv))
        raise SkipTest # TODO: implement your test here

class TestPypeDistributibleTask:
    def test_pype_distributible_task(self):
        # assert_equal(expected, PypeDistributibleTask(*argv, **kwargv))
        raise SkipTest # TODO: implement your test here


class TestTimeStampCompare:
    def test_time_stamp_compare(self):
        # assert_equal(expected, timeStampCompare(inputDataObjs, outputDataObjs, parameters))
        raise SkipTest # TODO: implement your test here

class TestPypeTaskCollectionBase:
    def test___init__(self):
        # pype_task_collection_base = PypeTaskCollectionBase(URL, tasks)
        raise SkipTest # TODO: implement your test here

    def test_getTasks(self):
        # pype_task_collection_base = PypeTaskCollectionBase(URL, tasks)
        # assert_equal(expected, pype_task_collection_base.getTasks())
        raise SkipTest # TODO: implement your test here

class TestPypeTaskCollection:
    def test___init__(self):
        # pype_task_collection = PypeTaskCollection(URL, tasks)
        raise SkipTest # TODO: implement your test here

    def test_addTask(self):
        # pype_task_collection = PypeTaskCollection(URL, tasks)
        # assert_equal(expected, pype_task_collection.addTask(task))
        raise SkipTest # TODO: implement your test here

    def test_getTasks(self):
        # pype_task_collection = PypeTaskCollection(URL, tasks)
        # assert_equal(expected, pype_task_collection.getTasks())
        raise SkipTest # TODO: implement your test here

class TestPypeScatteredTasks:

    def test_pype_scattered_tasks(self):
        import os
        #os.system("rm -rf /tmp/pypetest/*")
        nChunk = 5 

        infileObj =\
        pypeflow.data.PypeSplittableLocalFile(
                      "splittablefile://localhost/tmp/pypetest/test_in_1.txt", 
                      nChunk = nChunk)

        with open(infileObj.localFileName, "w") as f:
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
        infileObj.setScatterTask(PypeTask, PypeTaskBase, scatter)
        infileObj.getScatterTask()()
        
        def gather(*argv, **kwargv):
            inputObjs = sorted( kwargv["inputDataObjs"].items() )
            with open(kwargv["outputDataObjs"]["completeFile"].localFileName,"w") as outf:
                for k, subfile in inputObjs:
                    f = open(subfile.localFileName)
                    outf.write(f.read())
                    f.close()

        outfileObj =\
        pypeflow.data.PypeSplittableLocalFile(
                      "splittablefile://localhost/tmp/pypetest/test_out_1.txt", 
                      nChunk = nChunk)

        outfileObj.setGatherTask(PypeTask, PypeTaskBase, gather)

        PypeScatteredTasks = pypeflow.task.PypeScatteredTasks

        @PypeScatteredTasks( inputDataObjs = {"inf":infileObj},
                             outputDataObjs = {"outf":outfileObj} )
        def test_fun(*argv, **kwargv):
            chunk_id = kwargv["chunk_id"]
            self = test_fun[chunk_id]
            assert self.inf._path == "/tmp/pypetest/%03d_test_in_1.txt" % chunk_id
            with open(  self.outf._path , "w") as f:
                in_f = open(self.inf.localFileName,"r")
                f.write("out:"+in_f.read())
                in_f.close()
            return self.inf._path

        assert len(test_fun.getTasks()) == nChunk 
        for i in range(nChunk):
            test_fun[i]()

        outfileObj.getGatherTask()()
        
    def test_pype_scattered_tasks_2(self):
        import os
        #os.system("rm -rf /tmp/pypetest/*")

        nChunk = 5 

        infileObj =\
        pypeflow.data.PypeSplittableLocalFile(
                      "splittablefile://localhost/tmp/pypetest/test_in_2.txt", 
                      nChunk = nChunk)

        with open(infileObj.localFileName, "w") as f:
            for i in range(nChunk):
                f.write("file%02d\n" % i)

        with open("/tmp/pypetest/scatter.sh", "w") as f:
            f.write("#!/bin/bash\n")
            f.write("for f in %s;" % " ".join( ["%03d" % i for i in range(nChunk)] )) 
            f.write('do if [ -e /tmp/pypetest/%f"_test_in.txt" ];\
                        then rm /tmp/pypetest/$f"_test_in.txt"; fi;\n')
            f.write("done\n")
            for i in range(nChunk):
                f.write("echo file%02d > /tmp/pypetest/%03d_test_in_2.txt\n" % (i, i))

        PypeShellTask = pypeflow.task.PypeShellTask
        PypeTask = pypeflow.task.PypeTask
        PypeTaskBase = pypeflow.task.PypeTaskBase
        infileObj.setScatterTask(PypeShellTask, PypeTaskBase, "/tmp/pypetest/scatter.sh")
        infileObj.getScatterTask()()
        
        def gather(*argv, **kwargv):
            inputObjs = sorted( kwargv["inputDataObjs"].items() )
            with open(kwargv["outputDataObjs"]["completeFile"].localFileName,"w") as outf:
                for k, subfile in inputObjs:
                    f = open(subfile.localFileName)
                    outf.write("out:"+f.read())
                    f.close()

        outfileObj =\
        pypeflow.data.PypeSplittableLocalFile(
                      "splittablefile://localhost/tmp/pypetest/test_out_2.txt", 
                      nChunk = nChunk)

        outfileObj.setGatherTask(PypeTask, PypeTaskBase, gather)

        PypeScatteredTasks = pypeflow.task.PypeScatteredTasks

        @PypeScatteredTasks( inputDataObjs = {"inf":infileObj},
                             outputDataObjs = {"outf":outfileObj},
                             comment="xyz")
        def test_fun_2(*argv, **kwargv):
            assert kwargv["comment"] == "xyz"
            chunk_id = kwargv["chunk_id"]
            self = test_fun_2[chunk_id]
            assert self.inf._path == "/tmp/pypetest/%03d_test_in_2.txt" % chunk_id
            with open(  self.outf._path , "w") as f:
                f.write("file%02d\n" % chunk_id)
            return self.inf._path

        assert len(test_fun_2.getTasks()) == nChunk 
        for i in range(nChunk):
            test_fun_2[i]()

        outfileObj.getGatherTask()()

    def test_pype_scattered_tasks_3(self):
        import os
        #os.system("rm -rf /tmp/pypetest/*")
        nChunk = 5 


        infileObj0 =\
        pypeflow.data.PypeLocalFile(
                      "file://localhost/tmp/pypetest/test_in_0.txt")
        with open(infileObj0.localFileName,"w") as f:
            f.write("prefix:")

        infileObj =\
        pypeflow.data.PypeSplittableLocalFile(
                      "splittablefile://localhost/tmp/pypetest/test_in_3.txt", 
                      nChunk = nChunk)

        with open(infileObj.localFileName, "w") as f:
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
        infileObj.setScatterTask(PypeTask, PypeTaskBase, scatter)
        infileObj.getScatterTask()()
        
        def gather(*argv, **kwargv):
            inputObjs = sorted( kwargv["inputDataObjs"].items() )
            with open(kwargv["outputDataObjs"]["completeFile"].localFileName,"w") as outf:
                for k, subfile in inputObjs:
                    f = open(subfile.localFileName)
                    outf.write(f.read())
                    f.close()

        outfileObj3 =\
        pypeflow.data.PypeSplittableLocalFile(
                      "splittablefile://localhost/tmp/pypetest/test_out_3.txt", 
                      nChunk = nChunk)

        outfileObj3.setGatherTask(PypeTask, PypeTaskBase, gather)

        PypeScatteredTasks = pypeflow.task.PypeScatteredTasks

        @PypeScatteredTasks( inputDataObjs = {"inf":infileObj, "prefix":infileObj0},
                             outputDataObjs = {"outf":outfileObj3} )
        def test_fun_3(*argv, **kwargv):
            chunk_id = kwargv["chunk_id"]
            self = test_fun_3[chunk_id]

            assert self.inf._path == "/tmp/pypetest/%03d_test_in_3.txt" % chunk_id
            with open( self.prefix.localFileName, "r") as f:
                prefix = f.read()

            with open( self.outf._path, "w") as f:
                in_f = open(self.inf.localFileName,"r")
                f.write(prefix + in_f.read())
                in_f.close()
            return self.inf._path

        assert len(test_fun_3.getTasks()) == nChunk 
        for i in range(nChunk):
            test_fun_3[i]()

        outfileObj3.getGatherTask()()
