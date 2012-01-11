from nose import SkipTest
from nose.tools import assert_equal

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

