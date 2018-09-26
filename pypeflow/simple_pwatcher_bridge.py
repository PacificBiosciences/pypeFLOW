from __future__ import absolute_import
from .util import (mkdirs, system, touch, run, cd)
import pwatcher.blocking
import pwatcher.fs_based
import pwatcher.network_based
import networkx
import networkx.algorithms.dag #import (topological_sort, is_directed_acyclic_graph)

import hashlib
import json
import logging
import os
import pprint
import random
import re
import sys
import tempfile
import time
import traceback

LOG = logging.getLogger(__name__)

def generate_jobid(node, script_fn):
    # For now, we keep it simple. Just the task.json.
    # We truncate the job_name to 15 chars for the sake of some job systems.
    script_content = open(script_fn).read()
    checksum = hashlib.md5(script_content).hexdigest()
    return 'P' + checksum[0:14]

def generate_jobid_alt_given_checksum(script_fn, checksum):
    """
    Note: Unable to run job: denied: "0_raw_fofn_abs_" is not a valid object name (cannot start with a digit).
    So we need a prefix.

    >>> generate_jobid_alt_given_checksum('4-quiver/000000F_002/run.sh', '123')
    'P4_000000F_002_123'
    >>> generate_jobid_alt_given_checksum('4-quiver/quiver_scatter/000000F_002/run.sh', '456')
    'P4_000000F_002_456'
    """
    # TODO: Consider intermediate directories.
    stage = get_stage_char(script_fn)
    basedirname = os.path.basename(os.path.dirname(script_fn))
    return alphanum('P' + stage + '_' + basedirname + '_' + checksum) # without length limit

def generate_jobid_alt(node, script_fn):
    # dgordon suggests this as a preferable alternative.
    script_content = open(script_fn).read()
    checksum = hashlib.md5(script_content).hexdigest()
    return generate_jobid_alt_given_checksum(script_fn, checksum)

JOBID_GENERATORS = [generate_jobid, generate_jobid_alt]

def alphanum(foo):
    """
    >>> alphanum('/foo-bar/')
    '_foo_bar_'
    """
    return foo.replace('/', '_').replace('-', '_')

re_stage_char = re.compile(r'\b(\d)-\w+/')

def get_stage_char(fn):
    """
    >>> get_stage_char('0-hi/x/y')
    '0'
    >>> get_stage_char('x/y')
    'P'
    """
    mo = re_stage_char.search(fn)
    if mo:
        return mo.group(1)
    return 'P'

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
        for node in nodes:
            #node.satisfy() # This would do the job without a process-watcher.
            generated_script_fn = node.execute() # misnomer; this only dumps task.json now
            if not generated_script_fn:
                raise Exception('Missing generated_script_fn for Node {}'.format(node))
                ## This task is done.
                #endrun(node, 'EXIT 0')
                #self.__to_report.append(node)
                #continue
                # For now, consider it as "submitted" and finished.
                # (It would throw exception on error.)
            jobid = self.__generate_jobid(node, generated_script_fn)
            self.__known[jobid] = node

            rundir, basename = os.path.split(os.path.abspath(generated_script_fn))
            cmd = '/bin/bash {}'.format(basename)
            job_type = node.pypetask.parameters.get('job_type', None)
            dist = node.pypetask.dist # must always exist now
            job_dict = dict(self.__job_defaults_dict)
            job_dict.update(dist.job_dict)
            LOG.debug('In rundir={!r}, job_dict={!r}'.format(
                rundir, job_dict))
            #job_nproc = dist.NPROC # string or int
            #job_mb = dist.MB # string or int, megabytes
            job_local = int(dist.local) # bool->1/0, easily serialized
            jobids[jobid] = {
                'cmd': cmd,
                'rundir': rundir,
                # These are optional:
                'job_type': job_type,
                'job_dict': job_dict,
                #'job_nproc': job_nproc,
                #'job_mb': job_mb,
                'job_local': job_local,
            }
        # Also send the default type and queue-name.
        watcher_args = {
                'jobids': jobids,
                'job_type': self.__job_type,
                'job_defaults_dict': self.__job_defaults_dict,
        }
        result = self.watcher.run(**watcher_args)
        LOG.debug('Result of watcher.run()={}'.format(repr(result)))
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

    def __init__(self, watcher, section_job=None, job_type='local', job_defaults_dict=None, jobid_generator=0):
        self.watcher = watcher
        self.__section_job = section_job if section_job else {}
        self.__job_type = job_type
        self.__job_defaults_dict = job_defaults_dict
        self.__running = set() # jobids
        self.__known = dict() # jobid -> Node
        self.__to_report = list() # Nodes
        self.__generate_jobid = JOBID_GENERATORS[jobid_generator]

def get_unsatisfied_subgraph(g):
    unsatg = networkx.DiGraph(g)
    for n in g.nodes():
        if n.satisfied():
            unsatg.remove_node(n)
    return unsatg
def find_all_ancestors(g):
    ancestors = dict()
    def get_ancestors(node):
        """memoized"""
        if node not in ancestors:
            myancestors = set()
            for pred in g.predecessors_iter(node):
                myancestors.update(get_ancestors(pred))
            ancestors[node] = myancestors
        return ancestors[node]
    for node in g.nodes():
        get_ancestors(node)
    return ancestors
def find_next_ready_and_remove(g, node):
    """Given a recently satisfied node,
    return any successors with in_degree 1.
    Then remove node from g immediately.
    """
    ready = set()
    for n in g.successors(node):
        if 1 == g.in_degree(n):
            ready.add(n)
    g.remove_node(node)
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
        use_tmpdir = self.use_tmpdir
        pre_script = self.pre_script
        if pypetask.dist.use_tmpdir is not None:
            use_tmpdir = pypetask.dist.use_tmpdir
        node = PypeNode(pypetask.name, pypetask.wdir, pypetask, needs, use_tmpdir, pre_script)
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
    def refreshTargets(self, targets=None, updateFreq=10, exitOnFailure=True):
        try:
            self._refreshTargets(updateFreq, exitOnFailure)
        except:
            self.tq.terminate()
            raise
    def _refreshTargets(self, updateFreq, exitOnFailure):
        """Raise Exception (eventually) on any failure.
        - updateFreq (seconds) is really a max; we climb toward it gradually, and we reset when things change.
        - exitOnFailure=False would allow us to keep running (for a while) when parallel jobs fail.
        - self.max_jobs: the max number of concurrently running jobs
          - possibly this should be the number of cpus in use, but for now it is qsub jobs, for simplicity.
        """
        # Note: With the 'blocking' pwatcher, we will have 1 thread per live qsub job.
        # If/when that becomes a problem, please re-write the pwatcher as Go or Nim.
        # This loop is single-threaded. If we ignore max_jobs,
        # then we would send large queries for no reason, but that is not really a big deal.
        # The Python 'blocking' pwatcher is the real reason to limit jobs, for now.
        assert networkx.algorithms.dag.is_directed_acyclic_graph(self.graph)
        assert isinstance(self.max_jobs, int)
        failures = 0
        unsatg = get_unsatisfied_subgraph(self.graph)
        ready = find_all_roots(unsatg)
        submitted = set()
        init_sleep_time = 0.1
        sleep_time = init_sleep_time
        slept_seconds_since_last_ping = 0.0
        num_iterations_since_last_ping = 0
        num_iterations_since_last_ping_max = 1
        LOG.info('Num unsatisfied: {}, graph: {}'.format(len(unsatg), len(self.graph)))
        while ready or submitted:
            # Nodes cannot be in ready or submitted unless they are also in unsatg.
            to_submit = set()
            if self.max_jobs <= 0:
                msg = 'self.max_jobs={}'.format(self.max_jobs)
                raise Exception(msg)
            while ready and (self.max_jobs > len(submitted) + len(to_submit)):
                node = ready.pop()
                to_submit.add(node)
                LOG.info('About to submit: {!r}'.format(node))
            if to_submit:
                unsubmitted = set(self.tq.enque(to_submit)) # In theory, this traps exceptions.
                if unsubmitted:
                    msg = 'Failed to enqueue {} of {} jobs: {!r}'.format(
                        len(unsubmitted), len(to_submit), unsubmitted)
                    LOG.error(msg)
                    #ready.update(unsubmitted) # Resubmit only in pwatcher, if at all.
                    raise Exception(msg)
                submitted.update(to_submit - unsubmitted)
            LOG.debug('N in queue: {} (max_jobs={})'.format(len(submitted), self.max_jobs))
            recently_done = set(self.tq.check_done())
            num_iterations_since_last_ping += 1
            if not recently_done:
                if not submitted:
                    LOG.warning('Nothing is happening, and we had {} failures. Should we quit? Instead, we will just sleep.'.format(failures))
                    #break
                if num_iterations_since_last_ping_max <= num_iterations_since_last_ping:
                    # Ping!
                    LOG.info('(slept for another {}s -- another {} loop iterations)'.format(
                        slept_seconds_since_last_ping, num_iterations_since_last_ping))
                    slept_seconds_since_last_ping = 0.0
                    num_iterations_since_last_ping = 0
                    num_iterations_since_last_ping_max += 1
                slept_seconds_since_last_ping += sleep_time
                time.sleep(sleep_time)
                sleep_time = sleep_time + 0.1 if (sleep_time < updateFreq) else updateFreq
                continue
            LOG.debug('recently_done: {!r}'.format([(rd, rd.satisfied()) for rd in recently_done]))
            LOG.debug('Num done in this iteration: {}'.format(len(recently_done)))
            sleep_time = init_sleep_time
            submitted -= recently_done
            recently_satisfied = set(n for n in recently_done if n.satisfied())
            recently_done -= recently_satisfied
            #LOG.debug('Now N recently_done: {}'.format(len(recently_done)))
            for node in recently_satisfied:
                ready.update(find_next_ready_and_remove(unsatg, node))
            if recently_satisfied:
                LOG.info('recently_satisfied:\n{}'.format(pprint.pformat(recently_satisfied)))
                LOG.info('Num satisfied in this iteration: {}'.format(len(recently_satisfied)))
                LOG.info('Num still unsatisfied: {}'.format(len(unsatg)))
            if recently_done:
                msg = 'Some tasks are recently_done but not satisfied: {!r}'.format(recently_done)
                LOG.error(msg)
                LOG.error('ready: {!r}\n\tsubmitted: {!r}'.format(ready, submitted))
                failures += len(recently_done)
                if exitOnFailure:
                    raise Exception(msg)
        assert not submitted
        assert not ready
        if failures:
            raise Exception('We had {} failures. {} tasks remain unsatisfied.'.format(
                failures, len(unsatg)))

    @property
    def max_jobs(self):
        return self.__max_njobs
    @max_jobs.setter
    def max_jobs(self, val):
        pre = self.__max_njobs
        if pre != int(val):
            self.__max_njobs = int(val)
            LOG.info('Setting max_jobs to {}; was {}'.format(self.__max_njobs, pre))
    def __init__(self, watcher, job_type, job_defaults_dict, max_jobs, use_tmpdir, squash, jobid_generator,
            pre_script=None,
        ):
        self.graph = networkx.DiGraph()
        # TODO: Inject PwatcherTaskQueue
        self.tq = PwatcherTaskQueue(watcher=watcher, job_type=job_type, job_defaults_dict=job_defaults_dict,
                jobid_generator=jobid_generator,
                )
        assert isinstance(max_jobs, int)
        assert max_jobs > 0, 'max_jobs needs to be set. If you use the "blocking" process-watcher, it is also the number of threads.'
        self.__max_njobs = None
        self.max_jobs = max_jobs
        self.sentinels = dict() # sentinel_done_fn -> Node
        self.pypetask2node = dict()
        self.use_tmpdir = use_tmpdir
        if not pre_script:
            pre_script = os.environ.get('PYPEFLOW_PRE', None)
            if pre_script:
                LOG.warning('Found PYPEFLOW_PRE in env; using that for pre_script.')
        self.pre_script = pre_script # command(s) to run at top of bash scripts
        if self.pre_script:
            LOG.info('At top of bash scripts, we will run:\n{}'.format(self.pre_script))
        self.squash = squash # This really should depend on each Task, but for now a global is fine.
        # For small genomes, serial tasks should always be squashed.

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
export PATH=$PATH:/bin
cd {wdir}
/bin/bash {rel_actual_script_fn}
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
class PypeNode(NodeBase):
    """
    We will clean this up later. For now, it is pretty tightly coupled to PypeTask.
    """
    def generate_script(self):
        wdir = os.path.normpath(self.wdir)
        mkdirs(wdir)
        pt = self.pypetask
        assert pt.wdir == self.wdir
        inputs = {k:os.path.relpath(v.path, wdir) for k,v in pt.inputs.items()}
        outputs = {k:os.path.relpath(v.path, wdir) for k,v in pt.outputs.items()}
        for v in outputs.values():
            assert not os.path.isabs(v), '{!r} is not relative'.format(v)
        bash_template = pt.bash_template
        if bash_template is not None:
            # Write bash_template.
            bash_script_fn = os.path.join(wdir, 'template.sh')
            with open(bash_script_fn, 'w') as ofs:
                message = '# Substitution will be similar to snakemake "shell".\n'
                if self.pre_script:
                    message += self.pre_script + '\n'
                ofs.write(message + bash_template)
            task_desc = {
                    'inputs': inputs,
                    'outputs': outputs,
                    'parameters': pt.parameters,
                    'bash_template_fn' : 'template.sh',
            }
        else:
            raise Exception('We no longer support python functions as PypeTasks.')
        task_content = json.dumps(task_desc, sort_keys=True, indent=4, separators=(',', ': ')) + '\n'
        task_json_fn = os.path.join(wdir, 'task.json')
        open(task_json_fn, 'w').write(task_content)
        python = 'python2.7' # sys.executable fails sometimes because of binwrapper: SE-152
        tmpdir_flag = '--tmpdir {}'.format(self.use_tmpdir) if self.use_tmpdir else ''
        cmd = '{} -m pypeflow.do_task {} {}'.format(python, tmpdir_flag, task_json_fn)
        env_setup = 'env | sort'
        if self.pre_script:
            env_setup = self.pre_script + '\n' + env_setup
        script_content = """#!/bin/bash
onerror () {{
  set -vx
  echo "FAILURE. Running top in $(pwd) (If you see -terminal database is inaccessible- you are using the python bin-wrapper, so you will not get diagnostic info. No big deal. This process is crashing anyway.)"
  rm -f top.txt
  which python
  which top
  env -u LD_LIBRARY_PATH top -b -n 1 >| top.txt &
  env -u LD_LIBRARY_PATH top -b -n 1 2>&1
  pstree -apl
}}
trap onerror ERR
{env_setup}

echo "HOSTNAME=$(hostname)"
echo "PWD=$(pwd)"

time {cmd}
""".format(**locals())
        script_fn = os.path.join(wdir, 'task.sh')
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
    def __init__(self, name, wdir, pypetask, needs, use_tmpdir, pre_script):
        super(PypeNode, self).__init__(name, wdir, needs)
        self.pypetask = pypetask
        self.use_tmpdir = use_tmpdir
        self.pre_script = pre_script

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
    assert os.path.isabs(path)
    basename = os.path.basename(path)
    basedir = os.path.relpath(os.path.dirname(path))
    producer = PRODUCERS.get(basedir)
    if producer is None:
        msg = 'No producer PypeTask for basedir {!r} from path {!r} -- Pure input?'.format(
            basedir, path)
        LOG.debug(msg)
        return PypeLocalFile(path, None)
    siblings = producer.outputs
    for plf in siblings.values():
        sibling_basename = os.path.basename(plf.path)
        if sibling_basename == basename:
            return plf
    raise Exception('Failed to find a PypeLocalFile for {!r} among outputs of {!r}\n\t(Note that pure inputs must not be in task directories.)'.format(
        path, producer))
def find_work_dir(paths):
    """Return absolute path to directory of all these paths.
    Must be same for all.
    """
    dirnames = set(os.path.dirname(os.path.normpath(p)) for p in paths)
    if len(dirnames) != 1:
        raise Exception('Cannot find work-dir for paths in multiple (or zero) dirs: {} in {}'.format(
            pprint.pformat(paths), pprint.pformat(dirnames)))
    d = dirnames.pop()
    return os.path.abspath(d)
class PypeLocalFile(object):
    def __repr__(self):
        if self.producer:
            path = os.path.relpath(self.path, self.producer.wdir)
            return 'PLF({!r}, {!r})'.format(path, os.path.relpath(self.producer.wdir))
        else:
            path = os.path.relpath(self.path)
            wdir = None
            return 'PLF({!r}, {!r})'.format(path, None)
    def __init__(self, path, producer):
        self.path = os.path.abspath(path)
        self.producer = producer
def makePypeLocalFile(p, producer=None):
    return PypeLocalFile(p, producer)
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
class Dist(object):
    @property
    def pypeflow_nproc(self):
        return self.job_dict['NPROC']
    @property
    def pypeflow_mb(self):
        """per processor"""
        return self.job_dict['MB']
    def __init__(self, NPROC=1, MB=4000, local=False, job_dict={}, use_tmpdir=None):
        my_job_dict = {}
        if local:
            # Keep it simple. If we run local-only, then do not even bother with tmpdir.
            # This helps with simpler scatter/gather tasks, which copy paths.
            use_tmpdir = False
        my_job_dict['NPROC'] = NPROC
        my_job_dict['MB'] = MB
        my_job_dict.update(job_dict) # user overrides everything
        self.job_dict = my_job_dict
        self.local = local
        self.use_tmpdir = use_tmpdir
def PypeTask(inputs, outputs, parameters=None, wdir=None, bash_template=None, dist=None):
    """A slightly messy factory because we want to support both strings and PypeLocalFiles, for now.
    This can alter dict values in inputs/outputs if they were not already PypeLocalFiles.
    """
    if dist is None:
        dist = Dist()
    LOG.debug('New PypeTask(wdir={!r},\n\tinputs={!r},\n\toutputs={!r})'.format(
        wdir, inputs, outputs))
    if wdir is None:
        #wdir = parameters.get('wdir', name) # One of these must be a string!
        wdir = find_work_dir([only_path(v) for v in outputs.values()])
        # Since we derived wdir from outputs, any relative paths should become absolute.
        for k,v in outputs.items():
            if not isinstance(v, PypeLocalFile) and not os.path.isabs(v):
                outputs[k] = os.path.abspath(v)
        for k,v in inputs.items():
            if not isinstance(v, PypeLocalFile) and not os.path.isabs(v):
                inputs[k] = os.path.abspath(v)
    else:
        # relpaths are relative to the provided wdir,
        pass
    if not os.path.isabs(wdir):
        wdir = os.path.abspath(wdir)
    this = _PypeTask(inputs, outputs, wdir, parameters, bash_template, dist)
    #basedir = os.path.basename(wdir)
    basedir = this.name
    if basedir in PRODUCERS:
        raise Exception('Basedir {!r} already used for {!r}. Cannot create new PypeTask {!r}.'.format(
            basedir, PRODUCERS[basedir], this))
    LOG.debug('Added PRODUCERS[{!r}] = {!r}'.format(basedir, this))
    PRODUCERS[basedir] = this
    this.canonicalize() # Already abs, but make everything a PLF.
    this.assert_canonical()
    LOG.debug('Built {!r}'.format(this))
    return this
class _PypeTask(object):
    """Adaptor from old PypeTask API.
    """
    def canonicalize(self):
        for key, val in self.outputs.items():
            if not isinstance(val, PypeLocalFile):
                # If relative, then it is relative to our wdir.
                #LOG.warning('Making PLF: {!r} {!r}'.format(val, self))
                if not os.path.isabs(val):
                    val = os.path.join(self.wdir, val)
                #LOG.warning('Muking PLF: {!r} {!r}'.format(val, self))
                val = PypeLocalFile(val, self)
                self.outputs[key] = val
            else:
                val.producer = self
        for key, val in self.inputs.items():
            if not isinstance(val, PypeLocalFile):
                assert os.path.isabs(val), 'Inputs cannot be relative at self point: {!r} in {!r}'.format(val, self)
                self.inputs[key] = findPypeLocalFile(val)
    def assert_canonical(self):
        # Output values will be updated after PypeTask init, so refer back to self as producer.
        for k,v in self.inputs.iteritems():
            assert os.path.isabs(v.path), 'For {!r}, input {!r} is not absolute'.format(self.wdir, v)
        for k,v in self.outputs.iteritems():
            assert os.path.isabs(v.path), 'For {!r}, output {!r} is not absolute'.format(self.wdir, v)
        common = set(self.inputs.keys()) & set(self.outputs.keys())
        assert (not common), 'A key is used for both inputs and outputs of PypeTask({}), which could be ok, but only if we refer to them as input.foo and output.foo in the bash script: {!r}'.format(self.wdir, common)
    def __call__(self, func):
        self.func = func
        self.__name__ = '{}.{}'.format(func.__module__, func.__name__)
        return self
    def __repr__(self):
        return 'PypeTask({!r}, {!r}, {!r}, {!r})'.format(self.name, self.wdir, pprint.pformat(self.outputs), pprint.pformat(self.inputs))
    def __init__(self, inputs, outputs, wdir, parameters, bash_template, dist):
        if parameters is None:
            parameters = {}
        name = os.path.relpath(wdir)
        URL = 'task://localhost/{}'.format(name)
        self.inputs = inputs
        self.outputs = outputs
        self.parameters = dict(parameters) # Always copy this, so caller can re-use, for convenience.
        self.bash_template = bash_template
        self.wdir = wdir
        self.name = name
        self.URL = URL
        self.dist = dist
        #for key, bn in inputs.iteritems():
        #    setattr(self, key, os.path.abspath(bn))
        #for key, bn in outputs.iteritems():
        #    setattr(self, key, os.path.abspath(bn))
        assert self.wdir, 'No wdir for {!r} {!r}'.format(self.name, self.URL)
        LOG.debug('Created {!r}'.format(self))


MyFakePypeThreadTaskBase = None  # just a symbol, not really used

# Here is the main factory.
def PypeProcWatcherWorkflow(
        URL = None,
        job_defaults=dict(),
        squash = False,
        **attributes):
    """Factory for the workflow.
    """
    job_type = job_defaults.get('job_type', 'local')
    job_name_style = job_defaults.get('job_name_style', '')
    watcher_type = job_defaults.get('pwatcher_type', 'fs_based')
    watcher_directory = job_defaults.get('pwatcher_directory', 'mypwatcher')
    use_tmpdir = job_defaults.get('use_tmpdir', None)
    max_jobs = int(job_defaults.get('njobs', 24)) # must be > 0, but not too high
    if watcher_type == 'blocking':
        pwatcher_impl = pwatcher.blocking
    elif watcher_type == 'network_based':
        pwatcher_impl = pwatcher.network_based
    else:
        pwatcher_impl = pwatcher.fs_based
    LOG.info('In simple_pwatcher_bridge, pwatcher_impl={!r}'.format(pwatcher_impl))
    watcher = pwatcher_impl.get_process_watcher(watcher_directory)
    if use_tmpdir:
        try:
            if not os.path.isabs(use_tmpdir):
                use_tmpdir = os.path.abspath(use_tmpdir)
        except (TypeError, AttributeError):
            use_tmpdir = tempfile.gettempdir()
    if not job_name_style:
        job_name_style = 0
    LOG.info('job_type={!r}, (default)job_defaults={!r}, use_tmpdir={!r}, squash={!r}, job_name_style={!r}'.format(
        job_type, job_defaults, use_tmpdir, squash, job_name_style,
    ))
    jobid_generator = int(job_name_style)
    return Workflow(watcher,
            job_type=job_type, job_defaults_dict=job_defaults, max_jobs=max_jobs, use_tmpdir=use_tmpdir,
            squash=squash, jobid_generator=jobid_generator,
    )
    #th = MyPypeFakeThreadsHandler('mypwatcher', job_type, job_queue)
    #mq = MyMessageQueue()
    #se = MyFakeShutdownEvent() # TODO: Save pwatcher state on ShutdownEvent. (Not needed for blocking pwatcher. Mildly useful for fs_based.)
    #return pypeflow.controller._PypeConcurrentWorkflow(URL=URL, thread_handler=th, messageQueue=mq, shutdown_event=se,
    #        attributes=attributes)

__all__ = ['PypeProcWatcherWorkflow', 'fn', 'makePypeLocalFile', 'MyFakePypeThreadTaskBase', 'PypeTask']
