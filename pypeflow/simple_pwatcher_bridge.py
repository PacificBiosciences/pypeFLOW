from .util import (mkdirs, system, touch, run, cd)
import pwatcher.blocking
import pwatcher.fs_based
import pwatcher.network_based
import networkx
import networkx.algorithms.dag #import (topological_sort, is_directed_acyclic_graph)

import collections
import hashlib
import json
import logging
import os
import pprint
import random
import sys
import time

LOG = logging.getLogger(__name__)

def generate_jobid(node, script_fn):
    # For now, we keep it simple. Just the task.json.
    script_content = open(script_fn).read()
    checksum = hashlib.md5(script_content).hexdigest()
    return 'P' + checksum[0:14]

def endrun(thisnode, status):
    """By convention for now, status is one of:
        'DEAD'
        'UNSUBMITTED' (a pseudo-status defined in the ready-loop of alive())
        'EXIT rc'
    """
    name = status.split()[0]
    if name == 'DEAD':
        LOG.warning(''.join(traceback.format_stack()))
        LOG.error('Task {}\n is DEAD, meaning no HEARTBEAT, but this can be a race-condition. If it was not killed, then restarting might suffice. Otherwise, you might have excessive clock-skew.'.format(thisnode))
        #thisnode.setSatisfied(False) # I think we can leave it unsatisfied. Not sure what happens on retry though.
    elif name == 'UNSUBMITTED':
        LOG.warning(''.join(traceback.format_stack()))
        LOG.error('Task {}\n is UNSUBMITTED, meaning job-submission somehow failed. Possibly a re-start would work. Otherwise, you need to investigate.'.format(thisnode))
        thisnode.setSatisfied(False)
    elif name != 'EXIT':
        raise Exception('Unexpected status {!r}'.format(name))
    else:
        code = int(status.split()[1])
        if 0 != code:
            LOG.error('Task {} failed with exit-code={}'.format(thisnode, code))
            thisnode.setSatisfied(False)
        else:
            thisnode.setSatisfied(True)

class PwatcherTaskQueue(object):
    def enque(self, nodes):
        """This should be an injected dependency.
        Yield any Nodes that could not be submitted.
        Nodes that lacked a script were actually run in-process and are considered
        successful unless they raised an Exception, so they go into the to_report set.
        """
        # Start all "nodes".
        # Note: It is safe to run this block always, but we save a
        # call to pwatcher with 'if ready'.
        LOG.debug('enque nodes:\n%s' %pprint.pformat(nodes))
        jobids = dict()
        #sge_option='-pe smp 8 -q default'
        for node in nodes:
            #node.satisfy() # This would do the job without a process-watcher.
            mkdirs(node.wdir)
            generated_script_fn = node.execute() # misnomer; this only dumps task.json now
            if not generated_script_fn:
                raise Exception('Missing generated_script_fn for Node {}'.format(node))
                # This task is done.
                endrun(node, 'EXIT 0')
                self.__to_report.append(node)
                continue
                # For now, consider it as "submitted" and finished.
                # (It would throw exception on error.)
            jobid = generate_jobid(node, generated_script_fn)
            self.__known[jobid] = node

            rundir, basename = os.path.split(os.path.abspath(generated_script_fn))
            cmd = '/bin/bash {}'.format(basename)
            sge_option = node.pypetask.parameters.get('sge_option', None)
            job_type = node.pypetask.parameters.get('job_type', None)
            job_queue = node.pypetask.parameters.get('job_queue', None)
            job_nprocs = node.pypetask.parameters.get('job_nprocs', None)
            jobids[jobid] = {
                'cmd': cmd,
                'rundir': rundir,
                # These are optional:
                'job_type': job_type,
                'job_queue': job_queue,
                'job_nprocs': job_nprocs,
                'sge_option': sge_option,
            }
        # Also send the default type and queue-name.
        watcher_args = {
                'jobids': jobids,
                'job_type': self.__job_type,
                'job_queue': self.__job_queue,
        }
        result = self.watcher.run(**watcher_args)
        LOG.debug('Result of watcher.run()={}'.format(repr(result)))
        submitted = result['submitted']
        self.__running.update(submitted)
        #log.info("QQQ ADDED: {}".format(jobid))
        for jobid in (set(jobids.keys()) - set(submitted)):
            yield self.__known[jobid] # TODO: What should be the status for these submission failures?

    def check_done(self):
        """Yield Nodes which have finished since the last check.
        """
        watcher_args = {
            'jobids': list(self.__running),
            'which': 'list',
        }
        q = self.watcher.query(**watcher_args)
        #LOG.debug('In check_done(), result of query:%s' %repr(q))
        for jobid, status in q['jobids'].iteritems():
            if status.startswith('EXIT') or status.startswith('DEAD'):
                self.__running.remove(jobid)
                node = self.__known[jobid]
                self.__to_report.append(node)
                try:
                    endrun(node, status)
                except Exception as e:
                    msg = 'Failed to clean-up FakeThread: jobid={} status={}'.format(jobid, repr(status))
                    LOG.exception(msg)
                    raise
        to_report = list(self.__to_report)
        self.__to_report = list()
        return to_report

    def terminate(self):
        watcher_args = {
            'jobids': list(self.__running),
            'which': 'known',
        }
        q = self.watcher.delete(**watcher_args)
        LOG.debug('In notifyTerminate(), result of delete:%s' %repr(q))

    def __init__(self, watcher, job_type='local', job_queue=None):
        self.watcher = watcher
        self.__job_type = job_type
        self.__job_queue = job_queue
        #self.__jobq = collections.deque()
        self.__running = set() # jobids
        self.__known = dict() # jobid -> Node
        self.__to_report = list() # Nodes

def get_unsatisfied_subgraph(g):
    unsatg = networkx.DiGraph(g)
    for n in g.nodes():
        if n.satisfied():
            unsatg.remove_node(n)
    return unsatg
def find_next_ready(g, node):
    """Given a recently satisfied node,
    return any successors with in_degree 1.
    """
    ready = set()
    for n in g.successors_iter(node):
        if 1 == g.in_degree(n):
            ready.add(n)
    return ready
def find_all_roots(g):
    """Find all nodes in g which have no predecessors.
    """
    roots = set()
    for node in g:
        if 0 == g.in_degree(node):
            roots.add(node)
    return roots

class Workflow(object):
    def _create_node(self, pypetask):
        """Given a PypeTask, return a Node for our Workflow graph.
        Recursively create more nodes based on 'pypetask.inputs',
        record them as 'node.needs', and update the global pypetask2node table.
        """
        needs = set()
        node = PypeNode(pypetask.name, pypetask.wdir, pypetask, needs) #, pypetask.generated_script_fn)
        self.pypetask2node[pypetask] = node
        for key, plf in pypetask.inputs.iteritems():
            if plf.producer is None:
                continue
            if plf.producer not in self.pypetask2node:
                self._create_node(plf.producer)
            needed_node = self.pypetask2node[plf.producer]
            needs.add(needed_node)
        LOG.debug('New {!r} needs {!r}'.format(node, needs))
        return node
    def addTask(self, pypetask):
        node = self._create_node(pypetask)
        sentinel_done_fn = node.sentinel_done_fn()
        if sentinel_done_fn in self.sentinels:
            msg = 'FOUND sentinel {!r} twice: {!r} ({!r}) and {!r}\nNote: Each task needs its own sentinel (and preferably its own run-dir).'.format(sentinel_done_fn, node, pypetask, self.sentinels[sentinel_done_fn])
            raise Exception(msg)
        self.sentinels[sentinel_done_fn] = node
        self.graph.add_node(node)
        for need in node.needs:
            #print "Need:", need, node
            self.graph.add_edge(need, node)
    def addTasks(self, tlist):
        for t in tlist:
            self.addTask(t)
    def refreshTargets(self, targets=None, updateFreq=10, exitOnFailure=True, max_concurrency=2):
        try:
            self._refreshTargets(updateFreq, exitOnFailure, max_concurrency)
        except:
            self.tq.terminate()
            raise
    def _refreshTargets(self, updateFreq, exitOnFailure, max_concurrency):
        """Raise Exception (eventually) on any failure.
        - updateFreq (seconds) is really a max; we climb toward it gradually, and we reset when things change.
        - max_concurrency is the number of simultaneous qsubs.
        - exitOnFailure=False would allow us to keep running (for a while) when parallel jobs fail.
        """
        assert networkx.algorithms.dag.is_directed_acyclic_graph(self.graph)
        failures = 0
        unsatg = get_unsatisfied_subgraph(self.graph)
        ready = find_all_roots(unsatg)
        queued = set()
        init_sleep_time = 0.1
        sleep_time = init_sleep_time
        LOG.info('Num unsatisfied: {}'.format(len(unsatg)))
        while unsatg:
            # Nodes cannot be in ready or queued unless they are also in unsatg.
            to_queue = set()
            while ready and max_concurrency > len(queued):
                node = ready.pop()
                to_queue.add(node)
                LOG.info('About to submit: {!r}'.format(node))
            if to_queue:
                unqueued = set(self.tq.enque(to_queue))
                #assert not unqueued, 'TODO: Decide what to do when enqueue fails.'
                if unqueued:
                    LOG.warning('Failed to enqueue {} of {} jobs: {!r}'.format(
                        len(unqueued), len(to_queue), unqueued))
                    ready.update(unqueued)
                queued.update(to_queue - unqueued)
            LOG.debug('N in queue: {}'.format(len(queued)))
            recently_done = set(self.tq.check_done())
            if not recently_done:
                if not queued:
                    LOG.warning('Nothing is happening, and we had {} failures. Should we quit? Instead, we will just sleep.'.format(failures))
                    #break
                LOG.info('sleep {}'.format(sleep_time))
                time.sleep(sleep_time)
                sleep_time = sleep_time + 0.1 if (sleep_time < updateFreq) else updateFreq
                continue
            LOG.debug('recently_done: {!r}'.format([(rd, rd.satisfied()) for rd in recently_done]))
            LOG.debug('Num done in this iteration: {}'.format(len(recently_done)))
            sleep_time = init_sleep_time
            queued -= recently_done
            recently_satisfied = set(n for n in recently_done if n.satisfied())
            recently_done -= recently_satisfied
            #LOG.debug('Now N recently_done: {}'.format(len(recently_done)))
            LOG.info('recently_satisfied: {!r}'.format(recently_satisfied))
            LOG.info('Num satisfied in this iteration: {}'.format(len(recently_satisfied)))
            for node in recently_satisfied:
                ready.update(find_next_ready(unsatg, node))
                unsatg.remove_node(node)
            LOG.info('Num still unsatisfied: {}'.format(len(unsatg)))
            if recently_done:
                msg = 'Some tasks are recently_done but not satisfied: {!r}'.format(recently_done)
                LOG.error(msg)
                failures += len(recently_done)
                if exitOnFailure:
                    raise Exception(msg)
        assert not queued
        assert not ready
        if failures:
            raise Exception('We had {} failures. {} tasks remain unsatisfied.'.format(
                failures, len(unsatg)))

    def __init__(self, watcher, job_type, job_queue):
        self.graph = networkx.DiGraph()
        self.tq = PwatcherTaskQueue(watcher=watcher, job_type=job_type, job_queue=job_queue) # TODO: Inject this.
        self.sentinels = dict() # sentinel_done_fn -> Node
        self.pypetask2node = dict()

class NodeBase(object):
    """Graph node.
    Can be satisfied on demand, but usually we call execute() and later run the script.
    """
    def setSatisfied(self, s):
        self.__satisfied = s
    def workdir(self):
        return self.wdir
    def script_fn(self):
        return os.path.join(self.workdir(), 'run.sh')
    def sentinel_done_fn(self):
        return self.script_fn() + '.done'
    def satisfied(self):
        """Not just done, but successful.
        If we see the sentinel file, then we memoize True.
        But if we finished a distributed job with exit-code 0, we do not need
        to wait for the sentinel; we know we had success.
        """
        #LOG.debug('Checking satisfied({!r}) for sentinel {!r}'.format(self, self.sentinel_done_fn()))
        if self.__satisfied is not None:
            return self.__satisfied
        if os.path.exists(self.sentinel_done_fn()):
            self.setSatisfied(True)
        return self.__satisfied == True
    def satisfy(self):
        if self.__satisfied:
            return
        # Technically, we might need to cd into wdir first.
        script_fn = self.execute()
        if script_fn:
            run(script_fn)
        self.__satisfied = True
    def execute(self):
        try:
            actual_script_fn = self.generate_script()
        except Exception:
            LOG.exception('Failed generate_script() for {!r}'.format(self))
            raise
        sentinel_done_fn = self.sentinel_done_fn()
        wdir = self.workdir()
        rel_actual_script_fn = os.path.relpath(actual_script_fn, wdir)
        wrapper = """#!/bin/sh
set -vex
cd {wdir}
bash {rel_actual_script_fn}
touch {sentinel_done_fn}
""".format(**locals())
        wrapper_fn = self.script_fn()
        #mkdirs(os.path.dirname(wrapper_fn))
        with open(wrapper_fn, 'w') as ofs:
            ofs.write(wrapper)
        return wrapper_fn
    def generate_script(self):
        raise NotImplementedError(repr(self))
    def __repr__(self):
        return 'Node({})'.format(self.name)
    def __init__(self, name, wdir, needs):
        self.__satisfied = None  # satisfiable
        self.name = name
        self.wdir = wdir
        self.needs = needs
class ComboNode(NodeBase):
    """Several Nodes to be executed in sequence.
    Only this ComboNode will be in the DiGraph, not the sub-Nodes.
    """
    def generate_script(self):
        raise NotImplementedError(repr(self))
    def __init__(self, nodes):
        #super(ComboNode, self).__init__(name, wdir, needs)
        self.nodes = nodes
class PypeNode(NodeBase):
    """
    We will clean this up later. For now, it is pretty tightly coupled to PypeTask.
    """
    def generate_script(self):
        pt = self.pypetask
        task_desc = {
                'inputs': {k:v.path for k,v in pt.inputs.items()},
                'outputs': {k:v.path for k,v in pt.outputs.items()},
                'parameters': pt.parameters,
                'python_function': pt.func_name,
        }
        task_content = json.dumps(task_desc, sort_keys=True, indent=4, separators=(',', ': '))
        task_json_fn = os.path.join(pt.wdir, 'task.json')
        open(task_json_fn, 'w').write(task_content)
        cmd = '{} -m pypeflow.do_task {}'.format(sys.executable, task_json_fn)
        script_content = """#!/bin/bash
{}
""".format(cmd)
        script_fn = os.path.join(pt.wdir, 'task.sh')
        open(script_fn, 'w').write(script_content)
        return script_fn
    def old_generate_script(self):
        pt = self.pypetask
        func = pt.func
        func(pt) # Run the function! Probably just generate a script.
        generated_script_fn = getattr(pt, 'generated_script_fn', None) # by convention
        try:
            # Maybe we should require a script always.
            generated_script_fn = pt.generated_script_fn
        except Exception:
            LOG.exception('name={!r} URL={!r}'.format(pt.name, pt.URL))
            raise
        return generated_script_fn
    def __init__(self, name, wdir, pypetask, needs): #, script_fn):
        super(PypeNode, self).__init__(name, wdir, needs)
        self.pypetask = pypetask

# This global exists only because we continue to support the old PypeTask style,
# where a PypeLocalFile does not know the PypeTask which produces it.
# (This also allows us to specify PypeTasks out of order, fwiw.)
# Someday, we might require PypeTasks to depend on outputs of other PypeTasks explicitly;
# then we can drop this dict.
PRODUCERS = dict()

def findPypeLocalFile(path):
    """Look-up based on tail dirname.
    Do not call this for paths relative to their work-dirs.
    """
    basename = os.path.basename(path)
    basedir = os.path.basename(os.path.dirname(path))
    producer = PRODUCERS.get(basedir)
    if producer is None:
        msg = 'Failed to find producer PypeTask for basedir {!r} from path {!r}'.format(
            basedir, path)
        log.debug(msg)
        return PypeLocalFile(path, None)
    siblings = producer.outputs
    for plf in siblings.values():
        sibling_basename = os.path.basename(plf.path)
        if sibling_basename == basename:
            return plf
    raise Exception('Failed to find a PypeLocalFile for {!r} among outputs of {!r}'.format(
        path, producer))
def find_work_dir(paths):
    """Return absolute path to directory of all these paths.
    Must be same for all.
    """
    dirnames = set(os.path.dirname(os.path.normpath(p)) for p in paths)
    if len(dirnames) != 1:
        raise Exception('Cannot find work-dir for paths in multiple (or zero) dirs: {!r}'.format(
            paths))
    d = dirnames.pop()
    return os.path.abspath(d)
class PypeLocalFile(object):
    def __repr__(self):
        return 'PLF({!r}, {!r}'.format(self.path, self.producer.wdir if self.producer else None)
    def __init__(self, path, producer=None):
        self.path = path
        self.producer = producer
def makePypeLocalFile(p):
    return PypeLocalFile(os.path.abspath(p))
def fn(p):
    """This must be run in the top run-dir.
    All task funcs are executed there.
    """
    if isinstance(p, PypeLocalFile):
        p = p.path
    return os.path.abspath(p)
def only_path(p):
    if isinstance(p, PypeLocalFile):
        return p.path
    else:
        return p
def PypeTask(inputs, outputs, TaskType, parameters=None, URL=None, wdir=None, name=None):
    """A slightly messy factory because we want to support both strings and PypeLocalFiles, for now.
    """
    inputs = dict(inputs)
    outputs = dict(outputs)
    if wdir is None:
        wdir = find_work_dir([only_path(v) for v in outputs.values()])
    this = _PypeTask(inputs, outputs, parameters, URL, wdir, name)
    #basedir = os.path.basename(wdir)
    basedir = this.name
    if basedir in PRODUCERS:
        raise Exception('Basedir {!r} already used for {!r}. Cannot create new PypeTask {!r}.'.format(
            basedir, PRODUCERS[basedir], this))
    PRODUCERS[basedir] = this
    for key, val in outputs.items():
        if not isinstance(val, PypeLocalFile):
            outputs[key] = PypeLocalFile(val, this)
        else:
            val.producer = this
    for key, val in inputs.items():
        if not isinstance(val, PypeLocalFile):
            inputs[key] = findPypeLocalFile(val)
    common = set(inputs.keys()) & set(outputs.keys())
    assert (not common), 'Keys in both inputs and outputs of PypeTask({}): {!r}'.format(wdir, common)
    return this
class _PypeTask(object):
    """Adaptor from old PypeTask API.
    """
    def __call__(self, func):
        self.func = func
        self.func_name = '{}.{}'.format(func.__module__, func.__name__)
        return self
    def __repr__(self):
        return 'PypeTask({!r}, {!r}, {!r}, {!r})'.format(self.name, self.wdir, pprint.pformat(self.outputs), pprint.pformat(self.inputs))
    def __init__(self, inputs, outputs, parameters=None, URL=None, wdir=None, name=None):
        if parameters is None:
            parameters = {}
        if wdir is None:
            wdir = parameters.get('wdir', name) # One of these must be a string!
        if name is None:
            name = os.path.relpath(wdir)
        if URL is None:
            URL = 'task://localhost/{}'.format(name)
        self.inputs = inputs
        self.outputs = outputs
        self.parameters = parameters
        self.wdir = wdir
        self.name = name
        self.URL = URL
        #for key, bn in inputs.iteritems():
        #    setattr(self, key, os.path.abspath(bn))
        #for key, bn in outputs.iteritems():
        #    setattr(self, key, os.path.abspath(bn))
        LOG.debug('Created {!r}'.format(self))

class DummyPypeTask(_PypeTask):
    def __init__(self):
        super(DummyPypeTask, self).__init__({}, {}, {}, wdir='/')

#ROOT_PYPE_TASK = DummyPypeTask()

MyFakePypeThreadTaskBase = None  # just a symbol, not really used

# Here is the main factory.
def PypeProcWatcherWorkflow(
        URL = None,
        job_type='local',
        job_queue='UNSPECIFIED_QUEUE',
        watcher_type='fs_based',
        watcher_directory='mypwatcher',
        **attributes):
    """Factory for the workflow.
    """
    if watcher_type == 'blocking':
        pwatcher_impl = pwatcher.blocking
    elif watcher_type == 'network_based':
        pwatcher_impl = pwatcher.network_based
    else:
        pwatcher_impl = pwatcher.fs_based
    LOG.warning('In simple_pwatcher_bridge, pwatcher_impl={!r}'.format(pwatcher_impl))
    LOG.info('In simple_pwatcher_bridge, pwatcher_impl={!r}'.format(pwatcher_impl))
    watcher = pwatcher_impl.get_process_watcher(watcher_directory)
    LOG.info('job_type={!r}, job_queue={!r}'.format(job_type, job_queue))
    return Workflow(watcher, job_type=job_type, job_queue=job_queue)
    #th = MyPypeFakeThreadsHandler('mypwatcher', job_type, job_queue)
    #mq = MyMessageQueue()
    #se = MyFakeShutdownEvent() # TODO: Save pwatcher state on ShutdownEvent. (Not needed for blocking pwatcher. Mildly useful for fs_based.)
    #return pypeflow.controller._PypeConcurrentWorkflow(URL=URL, thread_handler=th, messageQueue=mq, shutdown_event=se,
    #        attributes=attributes)
PypeProcWatcherWorkflow.setNumThreadAllowed = lambda x, y: None

__all__ = ['PypeProcWatcherWorkflow', 'fn', 'makePypeLocalFile', 'MyFakePypeThreadTaskBase', 'PypeTask']
