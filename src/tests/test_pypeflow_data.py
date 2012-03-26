from nose.tools import assert_equal
from nose import SkipTest
import tempfile
import pypeflow.data
import pypeflow.task
import os

PypeLocalFileCollection = pypeflow.data.PypeLocalFileCollection
PypeLocalFile = pypeflow.data.PypeLocalFile
fn = pypeflow.data.fn

class TestFn:
    def test_fn(self):
        file = PypeLocalFile("file://localhost/test1")
        assert fn(file) == "/test1"
        file = PypeLocalFile("file://localhost/test1/")
        assert fn(file) == "/test1/"
        file = PypeLocalFile("file://localhost/tmp/test1")
        assert fn(file) == "/tmp/test1"
        file = PypeLocalFile("file://localhost"+ os.path.abspath("./test1"))
        assert fn(file) == os.path.abspath("./test1") 

class TestPypeDataObjectBase: #this class can not be tested directly
    pass

class TestPypeLocalFile:
    def test___init__(self):
        obj = PypeLocalFile("file://localhost/test")
        assert fn(obj) == "/test"
        obj = PypeLocalFile("file://localhost/test", **{"x":123})
        assert obj.x == 123

    def test_clean(self):
        # pype_local_file = PypeLocalFile(URL, readOnly, **attributes)
        # assert_equal(expected, pype_local_file.clean())
        raise SkipTest # TODO: implement your test here

    def test_exists(self):
        obj = PypeLocalFile("file://localhost/tmp/pypetest/test")
        os.system("mkdir -p /tmp/pypetest/; touch /tmp/pypetest/test")
        assert obj.exists == True
        os.system("rm /tmp/pypetest/test")
        assert obj.exists == False


        # pype_local_file = PypeLocalFile(URL, readOnly, **attributes)
        # assert_equal(expected, pype_local_file.exists())
        raise SkipTest # TODO: implement your test here

    def test_path(self):
        # pype_local_file = PypeLocalFile(URL, readOnly, **attributes)
        # assert_equal(expected, pype_local_file.path())
        raise SkipTest # TODO: implement your test here

    def test_timeStamp(self):
        # pype_local_file = PypeLocalFile(URL, readOnly, **attributes)
        # assert_equal(expected, pype_local_file.timeStamp())
        raise SkipTest # TODO: implement your test here

    def test_verify(self):
        # pype_local_file = PypeLocalFile(URL, readOnly, **attributes)
        # assert_equal(expected, pype_local_file.verify())
        raise SkipTest # TODO: implement your test here

class TestPypeLocalFileColletion:

    def test___init__(self):
        files = PypeLocalFileCollection("files://localhost/tmp/pypetest/test1")
        assert files.URL == "files://localhost/tmp/pypetest/test1"
        assert files.localFileName == None

    def test_addLocalFile(self):
        files = PypeLocalFileCollection("files://localhost/tmp/pypetest/test1")
        aNewFile = PypeLocalFile("file://localhost/tmp/pypetest/test2")
        files.addLocalFile(aNewFile)
        assert files.localFileName == files.localFiles[0].localFileName 
        assert fn(files) == fn(files.localFiles[0])

    def test_timeStamp(self):
        raise SkipTest # TODO: implement your test here

    def exists(self):
        raise SkipTest # TODO: implement your test here

class TestPypeHDF5Dataset:
    pass

class TestPypeLocalCompositeFile:
    def test___init__(self):
        # pype_local_composite_file = PypeLocalCompositeFile(URL, readOnly, **attributes)
        raise SkipTest # TODO: implement your test here

class TestMakePypeLocalFile:
    def test_make_pype_local_file(self):
        # assert_equal(expected, makePypeLocalFile(aLocalFileName, readOnly, **attributes))
        raise SkipTest # TODO: implement your test here

class TestPypeSplittableLocalFile:
    def test___init__(self):
        pype_splittable_local_file =\
        pypeflow.data.PypeSplittableLocalFile("splittablefile://localhost/./test.txt", 
                                              nChunk=5)
        for i in range(5):
            assert pype_splittable_local_file._splittedFiles[i].URL ==\
            'file://localhost/./%03d_test.txt' % i

    def test_setGatherTask(self):

        for i in range(5):
            with open("/tmp/pypetest/%03d_test_fofn.txt" % i, "w") as f:
                f.write("file%02d\n" % i)

        pype_splittable_local_file =\
        pypeflow.data.PypeSplittableLocalFile("splittablefile://localhost/tmp/pypetest/test_fofn.txt", 
                                              nChunk=5)
        with open("/tmp/pypetest/gather.sh", "w") as f:
            f.write("#!/bin/bash\n")
            f.write("if [ -e /tmp/pypetest/test_fofn.txt ]; then rm /tmp/pypetest/test_fofn.txt; fi\n")
            f.write("for f in %s;" % " ".join( ["%03d" % i for i in range(5)] )) 
            f.write('do cat /tmp/pypetest/$f"_test_fofn.txt" >> /tmp/pypetest/test_fofn.txt\n')
            f.write("done\n")

        PypeShellTask = pypeflow.task.PypeShellTask
        PypeTaskBase = pypeflow.task.PypeTaskBase
        pype_splittable_local_file.setGatherTask(PypeShellTask, 
                                                 PypeTaskBase, 
                                                 "/tmp/pypetest/gather.sh")
        pype_splittable_local_file.getGatherTask()()

        with open("/tmp/pypetest/test_fofn.txt") as f:
            i = 0
            for l in f:
                l = l.strip()
                assert l == "file%02d" % i
                i += 1

        import os
        for i in range(5):
            os.system(" rm  /tmp/pypetest/%03d_test_fofn.txt" % i)

    def test_setScatterTask(self):
        
        with open("/tmp/pypetest/test_fofn.txt", "w") as f:
            for i in range(5):
                f.write("file%02d\n" % i)

        pype_splittable_local_file =\
        pypeflow.data.PypeSplittableLocalFile("splittablefile://localhost/tmp/pypetest/test_fofn.txt", 
                                              nChunk=5)

        with open("/tmp/pypetest/scatter.sh", "w") as f:
            f.write("#!/bin/bash\n")
            f.write("for f in %s;" % " ".join( ["%03d" % i for i in range(5)] )) 
            f.write('do if [ -e /tmp/pypetest/%f"_test_fofn.txt" ]; \
                        then rm /tmp/pypetest/$f"_test_fofn.txt"; fi\n')
            f.write("done\n")
            for i in range(5):
                f.write("echo file%02d > /tmp/pypetest/%03d_test_fofn.txt\n" % (i, i))

        PypeShellTask = pypeflow.task.PypeShellTask
        PypeTaskBase = pypeflow.task.PypeTaskBase
        pype_splittable_local_file.setScatterTask(PypeShellTask, 
                                                  PypeTaskBase, 
                                                  "/tmp/pypetest/scatter.sh")
        pype_splittable_local_file.getScatterTask()()

        for i in range(5):
            with open("/tmp/pypetest/%03d_test_fofn.txt" % i) as f:
                l = f.read().strip()
                assert l == "file%02d" % i


    def test_getGatherTask(self):
        pype_splittable_local_file =\
        pypeflow.data.PypeSplittableLocalFile("splittablefile://localhost/tmp/pypetest/test_fofn.txt", 
                                              nChunk=5)
        PypeShellTask = pypeflow.task.PypeShellTask
        PypeTaskBase = pypeflow.task.PypeTaskBase
        pype_splittable_local_file.setGatherTask(PypeShellTask, PypeTaskBase, "/tmp/pypetest/gather.sh")
        assert pype_splittable_local_file.getGatherTask() == pype_splittable_local_file._gatherTask
        assert pype_splittable_local_file.getScatterTask() == None

    def test_getScatterTask(self):
        pype_splittable_local_file =\
        pypeflow.data.PypeSplittableLocalFile("splittablefile://localhost/tmp/pypetest/test_fofn.txt", 
                                              nChunk=5)
        PypeShellTask = pypeflow.task.PypeShellTask
        PypeTaskBase = pypeflow.task.PypeTaskBase
        pype_splittable_local_file.setScatterTask(PypeShellTask, PypeTaskBase, "/tmp/pypetest/scatter.sh")
        #pype_splittable_local_file.getScatterTask()
        assert pype_splittable_local_file.getScatterTask() == pype_splittable_local_file._scatterTask
        assert pype_splittable_local_file.getGatherTask() == None

    def test_getSplittedFiles(self):
        pype_splittable_local_file =\
        pypeflow.data.PypeSplittableLocalFile("splittablefile://localhost/tmp/pypetest/test.txt", 
                                              nChunk=5)
        i = 0
        for f in pype_splittable_local_file.getSplittedFiles():
            assert f.URL ==\
            'file://localhost/tmp/pypetest/%03d_test.txt' % i
            i += 1
