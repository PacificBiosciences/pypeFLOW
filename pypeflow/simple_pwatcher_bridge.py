from pwatcher import fs_based
from .util import (mkdirs, system, touch, run, cd)
import networkx
import networkx.algorithms.dag #import (topological_sort, is_directed_acyclic_graph)

import collections
import logging
import os
import pprint
import random
import time

LOG = logging.getLogger()

# These globals exist only to support the old PypeTask API,
# which relies on inputs/outputs instead of direct Node dependencies.
PRODUCER = dict()
CONSUMERS = collections.defaultdict(set)

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
            jobid = node.jobid
            self.__known[jobid] = node
            generated_script_fn = node.execute()
            if not generated_script_fn:
                raise Exception('Missing generated_script_fn for Node {}'.format(node))
                # This task is done.
                endrun(node, 'EXIT 0')
                self.__to_report.append(node)
                continue
                # For now, consider it as "submitted" and finished.
                # (It would throw exception on error.)

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
        with fs_based.process_watcher(self.__state_directory) as watcher:
            result = watcher.run(**watcher_args)
            #LOG.debug('Result of watcher.run()={}'.format(repr(result)))
            submitted = result['submitted']
            self.__running.update(submitted)
            for jobid in (set(jobids.keys()) - set(submitted)):
                yield self.__known[jobid] # TODO: What should be the status for these submission failures?

    def check_done(self):
        """Yield Nodes which have finished since the last check.
        """
        watcher_args = {
            'jobids': list(self.__running),
            'which': 'list',
        }
        with fs_based.process_watcher(self.__state_directory) as watcher:
            q = watcher.query(**watcher_args)
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
        with fs_based.process_watcher(self.__state_directory) as watcher:
            q = watcher.delete(**watcher_args)
        LOG.debug('In notifyTerminate(), result of delete:%s' %repr(q))

    def __init__(self, state_directory="STATE", job_type='local', job_queue=None):
        self.__state_directory = state_directory
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
    def addTask(self, pypetask):
        node = create_node(pypetask)
        self.graph.add_node(node)
        for need in node.needs:
            print "Need:", need, node
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
        """Return the number of failures, unless exitOnFailure.
        - updateFreq (seconds) is really a max; we climb toward it gradually, and we reset when things change.
        - max_concurrency is the number of simultaneous qsubs.
        - exitOnFailure=False would allow us to keep running when parallel jobs fail.
        """
        assert networkx.algorithms.dag.is_directed_acyclic_graph(self.graph)
        failures = 0
        unsatg = get_unsatisfied_subgraph(self.graph)
        ready = find_all_roots(unsatg)
        queued = set()
        init_sleep_time = 0.1
        sleep_time = init_sleep_time
        while unsatg:
            # Nodes cannot be in ready or queued unless they are also in unsatg.
            to_queue = set()
            while ready and max_concurrency > len(queued):
                node = ready.pop()
                to_queue.add(node)
                LOG.debug('will queue: {!r}'.format(node))
            if to_queue:
                unqueued = set(self.tq.enque(to_queue))
                assert not unqueued, 'TODO: Decided what to do when enqueue fails.'
                queued.update(to_queue - unqueued)
            LOG.debug('N in queue: {}'.format(len(queued)))
            recently_done = set(self.tq.check_done())
            if not recently_done:
                time.sleep(sleep_time)
                sleep_time = sleep_time + 0.1 if (sleep_time < updateFreq) else updateFreq
                continue
            else:
                sleep_time = init_sleep_time
            queued -= recently_done
            #print "recently_done:", [(rd, rd.satisfied()) for rd in recently_done]
            recently_satisfied = set(n for n in recently_done if n.satisfied())
            recently_done -= recently_satisfied
            #print "Now N recently_done:", len(recently_done)
            LOG.debug('recently_satisfied: {!r}'.format(recently_satisfied))
            for node in recently_satisfied:
                ready.update(find_next_ready(unsatg, node))
                unsatg.remove_node(node)
            if recently_done:
                msg = 'Some tasks are recently_done but not satisfied: {!r}'.format(recently_done)
                LOG.error(msg)
                failures += len(recently_done)
                if exitOnFailure:
                    raise Exception(msg)
        assert not queued
        assert not ready
        return failures
    def __init__(self, job_type, job_queue):
        self.graph = networkx.DiGraph()
        self.tq = PwatcherTaskQueue(state_directory='mypwatcher', job_type=job_type, job_queue=job_queue) # TODO: Inject this.

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
        if self.__satisfied is not None:
            return self.__satisfied
        if os.path.exists(self.sentinel_done_fn()):
            self.__satisfied = True
        return self.__satisfied == True
    def satisfy(self):
        if self.__satisfied:
            return
        script_fn = self.execute()
        if script_fn:
            run(script_fn)
        self.__satisfied = True
    def execute(self):
        actual_script_fn = self.generate_script()
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
        return 'Node({}, {})'.format(self.name, self.jobid)
    def __init__(self, name, wdir, needs):
        self.__satisfied = None  # satisfiable
        self.name = name
        self.wdir = wdir
        self.needs = needs
        #self.script_fn = script_fn
        self.jobid = 'Pup{}'.format(random.randint(0, 1000000))
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
        func = pt.func
        func(pt) # Run the function! Probably just generate a script.
        print dir(pt)
        generated_script_fn = getattr(pt, 'generated_script_fn', None) # by convention
        try:
            # Maybe we should require a script always.
            generated_script_fn = pt.generated_script_fn
        except Exception:
            print pt.name, pt.URL
            raise
        return generated_script_fn
    def __init__(self, name, wdir, pypetask, needs): #, script_fn):
        super(PypeNode, self).__init__(name, wdir, needs)
        self.pypetask = pypetask

def create_node(pypetask):
    """Given a PypeTask, return a Node for our Workflow graph.
    """
    needs = set()
    node = PypeNode(pypetask.name, pypetask.wdir, pypetask, needs) #, pypetask.generated_script_fn)
    for key, path in pypetask.outputs.iteritems():
        PRODUCER[path] = node
        LOG.debug('{} produces {!r}'.format(node, path))
    for key, path in pypetask.inputs.iteritems():
        CONSUMERS[path].add(node)
        LOG.debug('{} consumes {!r}'.format(node, path))
        if path in PRODUCER:
            needs.add(PRODUCER[path])
        else:
            print "{} not in PRODUCER".format(path)
    LOG.debug(' Therefore, {} needs {}'.format(node, needs))
    return node

class PypeTask(object):
    """Adaptor from old PypeTask API.
    """
    def __call__(self, func):
        self.func = func
        return self
    def __init__(self, inputs, outputs, parameters, TaskType, URL=None, wdir=None, name=None):
        """Kind of a mess. inputs/outputs become actual attributes.
        Someday, we will change how Tasks are specified.
        """
        self.inputs = dict(inputs)
        self.outputs = dict(outputs)
        self.parameters = dict(parameters)
        self.URL = URL
        self.wdir = wdir
        if name:
            self.name = name
        else:
            dirnames = list(os.path.dirname(os.path.normpath(o)) for o in self.outputs.values())
            if len(dirnames) == 1:
                d = dirnames.pop()
                self.name = os.path.basename(d)
                if not self.wdir:
                    self.wdir = os.path.abspath(d)
            else:
                self.name = os.path.basename(self.URL)
        for key, bn in inputs.iteritems():
            setattr(self, key, bn)
        for key, bn in outputs.iteritems():
            setattr(self, key, bn)

MyFakePypeThreadTaskBase = None  # just a symbol, not really used
# A PypeLocalFile is just a string now.
def makePypeLocalFile(p): return p
def fn(p): return p
# Here is the main factory.
def PypeProcWatcherWorkflow(
        job_type='local',
        job_queue='dev',
        **attributes):
    """Factory for the workflow.
    """
    return Workflow(job_type=job_type, job_queue=job_queue)
    #th = MyPypeFakeThreadsHandler('mypwatcher', job_type, job_queue)
    #mq = MyMessageQueue()
    #se = MyFakeShutdownEvent()
    #return pypeflow.controller._PypeConcurrentWorkflow(URL=URL, thread_handler=th, messageQueue=mq, shutdown_event=se,
    #        attributes=attributes)

__all__ = ['PypeProcWatcherWorkflow', 'fn', 'makePypeLocalFile', 'MyFakePypeThreadTaskBase', 'PypeTask']
