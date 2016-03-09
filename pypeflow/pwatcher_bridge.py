"""Bridge pattern to adapt pypeFLOW with pwatcher.

This is a bit messy, but it avoids re-writing the useful bits
of pypeFLOW.

With PypeProcWatcherWorkflow, the refreshTargets() loop will
be single-threaded!
"""
from pwatcher import fs_based
from pypeflow.task import PypeTask, PypeThreadTaskBase, PypeTaskBase
import pypeflow.controller
import pypeflow.task
import collections
import datetime
import glob
import hashlib
import json
import logging
import os
import pprint
import re
import sys
import time
import traceback

log = logging.getLogger(__name__)

def PypeProcWatcherWorkflow(URL = None, job_type='local', **attributes):
    """Factory for the workflow using our new
    filesystem process watcher.
    """
    th = MyPypeFakeThreadsHandler('mypwatcher', job_type)
    mq = MyMessageQueue()
    se = MyFakeShutdownEvent()
    return pypeflow.controller._PypeConcurrentWorkflow(URL=URL, thread_handler=th, messageQueue=mq, shutdown_event=se,
            attributes=attributes)

PypeProcWatcherWorkflow.setNumThreadAllowed = pypeflow.controller._PypeConcurrentWorkflow.setNumThreadAllowed

class Fred(object):
    """Fake thread.
    """
    INITIALIZED = 10
    STARTED = 20
    RUNNING = 30
    JOINED = 40
    def is_alive(self):
        return self.__status in (Fred.STARTED, Fred.RUNNING)
    def start(self):
        self.__status = Fred.STARTED
        self.__th.enqueue(self)
    def join(self, timeout=None):
        # Maybe we should wait until the sentinel is visible in the filesystem?
        # Also, if we were STARTED but not RUNNING, then we never did anything!

        #assert self.__status is not Fred.JOINED # Nope. Might be called a 2nd time by notifyTerminate().
        self.__status = Fred.JOINED
    # And our own special methods.
    def task(self):
        return self.__target
    def generate(self):
        success = self.__target()
    def setTargetStatus(self, status):
        self.__target.setStatus(status)
    def endrun(self, status):
        """By convention for now, status is either
            'DEAD'
        or
            'EXIT rc'
        """
        name = status.split()[0]
        if name == 'DEAD':
            self.setTargetStatus(pypeflow.task.TaskFail) # for lack of anything better
        elif name != 'EXIT':
            raise Exception('Unexpected status {!r}'.format(name))
        else:
            code = int(status.split()[1])
            if 0 == code:
                self.__target.check_missing()
                # TODO: If missing, just leave the status as TaskInitialized?
            else:
                self.setTargetStatus(pypeflow.task.TaskFail) # for lack of anything better
        self.__target.finish()
    def __init__(self, target, th):
        assert isinstance(target, MyFakePypeThreadTaskBase)
        self.__target = target # taskObj?
        self.__th = th # thread handler
        self.__status = Fred.INITIALIZED

class MyMessageQueue(object):
    def empty(self):
        return not self.__msgq
    def get(self):
        return self.__msgq.popleft()
    def put(self, msg):
        self.__msgq.append(msg)
    def __init__(self):
        self.__msgq = collections.deque()

class MyFakeShutdownEvent(object):
    """I do not see any code which actually uses the
    shutdown_event, but if needed someday, we can use this.
    """
    def set(self):
        pass

class MyPypeFakeThreadsHandler(object):
    """Stateless method delegator, for injection.
    """
    def create(self, target):
        thread = Fred(target=target, th=self)
        return thread
    def alive(self, threads):
        ready = dict()
        while self.__jobq:
            fred = self.__jobq.popleft()
            log.info("FRED:%s"%repr(fred))
            taskObj = fred.task()
            fred.generate() # -> taskObj->generated_script_fn by convention
            log.info('param:\n%s' %pprint.pformat(taskObj.parameters))
            try:
                script_fn = taskObj.generated_script_fn # BY CONVENTION
            except AttributeError:
                log.warning('Missing taskObj.generated_script_fn for task %s. Maybe we did not need it? Skipping and continuing.' %repr(taskObj))
                fred.endrun('EXIT 0')
                continue
            log.info('script_fn:%s' %repr(script_fn))
            content = open(script_fn).read()
            digest = hashlib.sha256(content).hexdigest()
            jobid = '{}'.format(digest)
            log.info('jobid=%s' %jobid)
            taskObj.jobid = jobid
            ready[jobid] = fred
            self.__known[jobid] = fred
        if ready:
            log.info('ready:\n%s' %pprint.pformat(ready))
        jobids = dict()
        for jobid, fred in ready.iteritems():
            cmd = '/bin/bash {}'.format(fred.task().generated_script_fn)
            jobids[jobid] = {
                'cmd': cmd,
                'rundir': '.',
            }
        watcher_args = {
                'jobids': jobids,
                'job_type': self.__job_type,
        }
        with fs_based.process_watcher(self.__state_directory) as watcher:
            watcher.run(**watcher_args)
        self.__running.update(set(jobids.keys()))

        watcher_args = {
            'jobids': list(self.__running),
            'which': 'list',
        }
        with fs_based.process_watcher(self.__state_directory) as watcher:
            q = watcher.query(**watcher_args)
        log.debug('In alive(), result of query:%s' %repr(q))
        for jobid, status in q['jobids'].iteritems():
            #log.debug('j={}, s={}'.format(jobid, status))
            if status.startswith('EXIT') or status.startswith('DEAD'):
                self.__running.remove(jobid)
                fred = self.__known[jobid]
                fred.endrun(status)
        #log.info('len(jobq)==%d' %len(self.__jobq))
        #log.info(''.join(traceback.format_stack()))
        return sum(thread.is_alive() for thread in threads)
    def join(self, threads, timeout):
        then = datetime.datetime.now()
        for thread in threads:
            #assert thread is not threading.current_thread()
            if thread.is_alive():
                to = max(0, timeout - (datetime.datetime.now() - then).seconds)
        # This is called only in the refreshTargets() catch, so
        # it can simply terminate all threads.
                thread.join(to)
        self.notifyTerminate(threads)
    def notifyTerminate(self, threads):
        """Assume these are daemon threads.
        We will attempt to join them all quickly, but non-daemon threads may
        eventually block the program from quitting.
        """
        pass #self.join(threads, 1)
        # TODO: Terminate only the jobs for 'threads'.
        # For now, use 'known' instead of 'infer' b/c we
        # also want to kill merely queued jobs, though that is currently difficult.
        watcher_args = {
            'jobids': list(self.__running),
            'which': 'known',
        }
        with fs_based.process_watcher(self.__state_directory) as watcher:
            q = watcher.delete(**watcher_args)
        log.debug('In notifyTerminate(), result of delete:%s' %repr(q))


    # And our special methods.
    def enqueue(self, fred):
        self.__jobq.append(fred)
    def __init__(self, state_directory, job_type):
        self.__state_directory = state_directory
        self.__job_type = job_type
        self.__jobq = collections.deque()
        self.__running = set()
        self.__known = dict()

class MyFakePypeThreadTaskBase(PypeThreadTaskBase):
    """Fake for PypeConcurrentWorkflow.
    It demands a subclass, even though we do not use threads at all.
    Here, we override everything that it overrides. PypeTaskBase defaults are fine.
    """
    @property
    def nSlots(self):
        """(I am not sure what happend if > 1, but we will not need that. ~cdunn)
        Return the required number of slots to run, total number of slots is determined by
        PypeThreadWorkflow.MAX_NUMBER_TASK_SLOT, increase this number by passing desired number
        through the "parameters" argument (e.g parameters={"nSlots":2}) to avoid high computationa
        intensive job running concurrently in local machine One can set the max number of thread
        of a workflow by PypeThreadWorkflow.setNumThreadAllowed()
        """
        try:
            nSlots = self.parameters["nSlots"]
        except AttributeError:
            nSlots = 1
        except KeyError:
            nSlots = 1
        return nSlots

    def setMessageQueue(self, q):
        self._queue = q

    def setShutdownEvent(self, e):
        self.shutdown_event = e

    def __call__(self, *argv, **kwargv):
        """Trap all exceptions, set fail flag, SEND MESSAGE, log, and re-raise.
        """
        try:
            return self.runInThisThread(*argv, **kwargv)
        except: # and re-raise
            log.exception('%s failed:\n%r' %(type(self).__name__, self))
            self._status = pypeflow.task.TaskFail  # TODO: Do not touch internals of base class.
            self._queue.put( (self.URL, pypeflow.task.TaskFail) )
            raise

    def runInThisThread(self, *argv, **kwargv):
        """
        Similar to the PypeTaskBase.run(), but it provide some machinary to pass information
        back to the main thread that run this task in a sepearated thread through the standard python
        queue from the Queue module.

        We expect this to be used *only* for tasks which generate run-scripts.
        Our version does not actually run the script. Instead, we expect the script-filename to be returned
        by run().
        """
        if self._queue == None:
            raise Exception('There seems to be a case when self.queue==None, so we need to let this block simply return.')
        self._queue.put( (self.URL, "started, runflag: %d" % True) )
        return self.run(*argv, **kwargv)

    # We must override this from PypeTaskBase b/c we do *not* produce outputs
    # immediately.
    def run(self, *argv, **kwargv):
        argv = list(argv)
        argv.extend(self._argv)
        kwargv.update(self._kwargv)

        inputDataObjs = self.inputDataObjs
        self.syncDirectories([o.localFileName for o in inputDataObjs.values()])

        outputDataObjs = self.outputDataObjs
        parameters = self.parameters

        log.info('Running task from function %s()' %(self._taskFun.__name__))
        rtn = self._runTask(self, *argv, **kwargv)

        if self.inputDataObjs != inputDataObjs or self.parameters != parameters:
            raise TaskFunctionError("The 'inputDataObjs' and 'parameters' should not be modified in %s" % self.URL)

        return True # to indicate that it run, since we no longer rely on runFlag

    def check_missing(self):
        timeout_s = 10
        sleep_s = .1
        self.syncDirectories([o.localFileName for o in self.outputDataObjs.values()]) # Sometimes helps in NFS.
        lastUpdate = datetime.datetime.now()
        while timeout_s > (datetime.datetime.now()-lastUpdate).seconds:
            missing = [(k,o) for (k,o) in self.outputDataObjs.iteritems() if not o.exists]
            if missing:
                log.debug("%s fails to generate all outputs; %s; missing:\n%s" %(
                    self.URL, repr(self._status),
                    pprint.pformat(missing),
                ))
                import commands
                cmd = 'pstree -pg -u cdunn'
                output = commands.getoutput(cmd)
                log.debug('`%s`:\n%s' %(cmd, output))
                dirs = set(os.path.dirname(o.localFileName) for o in self.outputDataObjs.itervalues())
                for d in dirs:
                    log.debug('listdir(%s): %s' %(d, repr(os.listdir(d))))
                #self._status = pypeflow.task.TaskFail
                time.sleep(sleep_s)
                sleep_s *= 2.0
            else:
                self._status = pypeflow.task.TaskDone
                break
        else:
            log.debug('timeout(%ss) in check_missing()' %timeout_s)
            self._status = pypeflow.task.TaskFail

    # And our own special methods.
    def finish(self):
        self.syncDirectories([o.localFileName for o in self.outputDataObjs.values()])
        self._queue.put( (self.URL, self._status) )
