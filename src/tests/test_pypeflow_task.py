from nose.tools import assert_equal
from nose import SkipTest
from pypeflow.task import *
from pypeflow.data import *

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

class TestPypeScatteredTask:
    def test_pype_scattered_task(self):
        file = PypeScatteredFile("sfile://localhost//tmp/test1", nChunk =5)
        assert file.URL == "sfile://localhost//tmp/test1"
        
        with open(fn(file),"w") as f:
            for i in range(5):
                f.write("file%03d\n" % i)
        
        with open("/tmp/split.sh","w") as f:
            for i in range(5):
                f.write("cat %s | awk 'NR-1 == %d {print}' > %s \n" % (fn(file), i, fn(file.getChunkFile(i))))
        
        outputDataObjs = dict(zip( [ "f1-%03d" % x for x in file.scatterFiles.keys()], file.scatterFiles.values()))
        
        task = PypeShellTask(inputDataObjs = {"f1": file},
                             outputDataObjs = outputDataObjs, 
                             URL="task://localshell/scatterTask1", 
                             TaskType=PypeThreadTaskBase) ( "/tmp/split.sh" )
        
        file.attachedScatterTask(task)
        
        file2 = PypeScatteredFile("sfile://localhost//tmp/test1", nChunk =5)
        scripts = {}
        for i in range(5):
            scriptName = "/tmp/proc%03d.sh" % i
            with open(scriptName, "w") as f:
                f.write("cat %s > %s" % ( fn( file.getChunkFile(i) ), fn( file2.getChunkFile(i) ) ) )
            scripts[i] = scriptName
        
        sTask = PypeScatteredShellTask( scatteredInputs = {"f1":file},
                                        scatteredOutputs = {"f2":file2},
                                      ) ( scripts )
        for i in range(5):
            subTask = sTask.getSubTask(i)
            assert subTask.URL == sTask.URL + "/subtask%03d" % i
            assert subTask.inputDataObjs["f1"] == file.getChunkFile(i)
            assert subTask.outputDataObjs["f2"] == file2.getChunkFile(i)
            assert fn(subTask.inputDataObjs["f1"]) == fn(file.getChunkFile(i))


class TestTimeStampCompare:
    def test_time_stamp_compare(self):
        # assert_equal(expected, timeStampCompare(inputDataObjs, outputDataObjs, parameters))
        raise SkipTest # TODO: implement your test here

