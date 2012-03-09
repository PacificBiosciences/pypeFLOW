from nose.tools import assert_equal
from nose import SkipTest

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

