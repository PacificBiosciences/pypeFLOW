from nose.tools import assert_equal
from nose import SkipTest
from pypeflow.data import *
from pypeflow.task import *
from Queue import Queue
import tempfile

class TestFn:
    def test_fn(self):
        # assert_equal(expected, fn(obj))
        raise SkipTest # TODO: implement your test here

class TestPypeDataObjectBase:
    def test___init__(self):
        # pype_data_object_base = PypeDataObjectBase(URL, **attributes)
        raise SkipTest # TODO: implement your test here

    def test_exists(self):
        # pype_data_object_base = PypeDataObjectBase(URL, **attributes)
        # assert_equal(expected, pype_data_object_base.exists())
        raise SkipTest # TODO: implement your test here

    def test_timeStamp(self):
        # pype_data_object_base = PypeDataObjectBase(URL, **attributes)
        # assert_equal(expected, pype_data_object_base.timeStamp())
        raise SkipTest # TODO: implement your test here

    def test___str__(self):
        # pype_local_file = PypeLocalFile(URL, readOnly, **attributes)
        # assert_equal(expected, pype_local_file.__str__())
        raise SkipTest # TODO: implement your test here

    def test_addVerifyFunction(self):
        # pype_local_file = PypeLocalFile(URL, readOnly, **attributes)
        # assert_equal(expected, pype_local_file.addVerifyFunction(verifyFunction))
        raise SkipTest # TODO: implement your test here

class TestPypeLocalFile:
    def test___init__(self):
        file = PypeLocalFile("file://localhost/test2")
        assert fn(file) == "test2"

    def test_clean(self):
        # pype_local_file = PypeLocalFile(URL, readOnly, **attributes)
        # assert_equal(expected, pype_local_file.clean())
        raise SkipTest # TODO: implement your test here

    def test_exists(self):
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
        files = PypeLocalFileCollection("files://localhost/./test1")
        assert files.URL == "files://localhost/./test1"
        assert files.localFileName == None

    def test_addLocalFile(self):
        files = PypeLocalFileCollection("files://localhost/./test1")
        aNewFile = PypeLocalFile("file://localhost/./test2")
        files.addLocalFile(aNewFile)
        assert files.localFileName == files.localFiles[0].localFileName 
        assert fn(files) == fn(files.localFiles[0])

    def test_timeStamp(self):
        raise SkipTest # TODO: implement your test here

    def exists(self):
        raise SkipTest # TODO: implement your test here

class TestPypeScatteredFile:

    def test___init__(self):
        file = PypeScatteredFile("sfile://localhost//xyz/test1", nChunk =5)
        assert file.URL == "sfile://localhost//xyz/test1"
        assert file.nChunk == 5
        assert fn(file.getChunkFile(0)) == "/xyz/test1-000-005"
        assert fn(file.getChunkFile(1)) == "/xyz/test1-001-005"
        assert fn(file.getChunkFile(2)) == "/xyz/test1-002-005"
        assert fn(file.getChunkFile(3)) == "/xyz/test1-003-005"
        assert fn(file.getChunkFile(4)) == "/xyz/test1-004-005"
        
    def test_attachedScatterTask(self):
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
        task.setMessageQueue(Queue())
        task()
        for i in range(5):
            with open(fn(file.getChunkFile(i))) as f:
                assert f.read().strip() == "file%03d" % i

    def test_attachedGatherTask(self):
        self.test_attachedScatterTask()
        file = PypeScatteredFile("sfile://localhost//tmp/test1", nChunk =5)
        os.system("mv %s %s_bak" % (fn(file), fn(file)))
        with open("/tmp/gather.sh","w") as f:
            for i in range(5):
                f.write("cat %s >> %s \n" % ( fn( file.getChunkFile(i) ), fn(file) ) )
        
        inputDataObjs = dict(zip( [ "f1-%03d" % x for x in file.scatterFiles.keys()], file.scatterFiles.values()))
        task = PypeShellTask(inputDataObjs = inputDataObjs,
                             outputDataObjs = {"f1": file}, 
                             URL="task://localshell/gatherTask1", 
                             TaskType=PypeThreadTaskBase) ( "/tmp/gather.sh" )
        task.setMessageQueue(Queue())
        task()
        with open(fn(file)) as f1:
            with open(fn(file)+"_bak") as f2:
                l1 = f1.read()
                l2 = f2.read()
                assert l1 == l2


class TestPypeHDF5Dataset:
    def test___init__(self):
        # pype_hd_f5_dataset = PypeHDF5Dataset(URL, readOnly, **attributes)
        raise SkipTest # TODO: implement your test here

class TestPypeLocalCompositeFile:
    def test___init__(self):
        # pype_local_composite_file = PypeLocalCompositeFile(URL, readOnly, **attributes)
        raise SkipTest # TODO: implement your test here

class TestMakePypeLocalFile:
    def test_make_pype_local_file(self):
        # assert_equal(expected, makePypeLocalFile(aLocalFileName, readOnly, **attributes))
        raise SkipTest # TODO: implement your test here

