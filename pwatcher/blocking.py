#!/usr/bin/env python2.7
"""Blocking process-watcher.

See fs_based.py. Here, delete is a no-op, and run() starts threads, so
the main program needs to wait for threads to finish somehow.

Typical submission_string:

    qsub -S /bin/bash -sync y -V -q production -N ${JOB_ID} \\\n -o "${STDOUT_FILE}" \\\n -e "${STDERR_FILE}" \\\n -pe smp ${NPROC} -l h_vmem=${MB}M \\\n "${CMD}"
"""
try:
    from shlex import quote
except ImportError:
    from pipes import quote
import collections
import contextlib
import copy
import glob
import json
import logging
import os
import pprint
import re
import signal
import string
import subprocess
import sys
import threading
import time
import traceback

log = logging.getLogger(__name__)

LOCAL_SUBMISSION_STRING = '/bin/bash -C ${CMD} >| ${STDOUT_FILE} 2>| ${STDERR_FILE}' # for job_local override
STATE_FN = 'state.py'
Job = collections.namedtuple('Job', ['jobid', 'cmd', 'rundir', 'options'])
MetaJob = collections.namedtuple('MetaJob', ['job', 'lang_exe'])
lang_python_exe = sys.executable
lang_bash_exe = '/bin/bash'

@contextlib.contextmanager
def cd(newdir):
    prevdir = os.getcwd()
    log.debug('CD: %r <- %r' %(newdir, prevdir))
    os.chdir(os.path.expanduser(newdir))
    try:
        yield
    finally:
        log.debug('CD: %r -> %r' %(newdir, prevdir))
        os.chdir(prevdir)

class MetaJobClass(object):
    ext = {
        lang_python_exe: '.py',
        lang_bash_exe: '.bash',
    }
    def get_wrapper(self):
        # Totally by convention, for now.
        return '%s/run-%s%s' %(self.mj.job.rundir, self.mj.job.jobid, self.ext[self.mj.lang_exe])
    def get_sentinel(self):
        return 'exit-%s' %self.mj.job.jobid # in watched dir
    def get_pid(self):
        return self.mj.pid
    def kill(self, pid, sig):
        stored_pid = self.get_pid()
        if not pid:
            pid = stored_pid
            log.info('Not passed a pid to kill. Using stored pid:%s' %pid)
        if pid and stored_pid:
            if pid != stored_pid:
                log.error('pid:%s != stored_pid:%s' %(pid, stored_pid))
        os.kill(pid, sig)
    def __init__(self, mj):
        self.mj = mj
class State(object):
    def notify_threaded(self, jobid):
        self.jobids_threaded.add(jobid)
    def notify_started(self, jobid):
        #state.top['jobids_submitted'].append(jobid)
        self.jobids_submitted.add(jobid)
        self.jobids_threaded.remove(jobid)
        log.debug('Thread notify_started({}).'.format(jobid))
    def notify_exited(self, jobid, rc):
        #self.top['jobid2exit'][jobid] = rc
        self.jobid2exit[jobid] = rc
        self.jobids_submitted.remove(jobid)
        log.debug('Thread notify_exited({}->{}).'.format(jobid, rc))
    def set_job(self, jobid, mjob):
        # Is this needed? For now, we are not actually saving state, so no.
        self.top['jobs'][jobid] = mjob
    def update_jobid2status(self, jobid2status):
        for jobid in self.jobids_threaded:
            status = 'THREADED'
            jobid2status[jobid] = status
        for jobid in self.jobids_submitted:
            status = 'RUNNING'
            # but actually it might not have started yet, or it could be dead, since we have blocking qsub calls
            jobid2status[jobid] = status
        for jobid, rc in self.jobid2exit.items():
            status = 'EXIT {}'.format(rc)
            jobid2status[jobid] = status
    def get_running_jobids(self):
        return list(self.jobids_submitted)
    def serialize(self):
        return pprint.pformat(self.top)
    @staticmethod
    def deserialize(directory, content):
        state = State(directory)
        state.top = eval(content)
        state.content_prev = content
        return state
    @staticmethod
    def create(directory):
        state = State(directory)
        #makedirs(state.get_directory_wrappers())
        #makedirs(state.get_directory_jobs())
        return state
    def __init__(self, directory):
        self.__directory = os.path.abspath(directory)
        self.content_prev = ''
        self.top = dict() # for serialization, when we decide we need it
        self.top['jobs'] = dict()
        #self.top['jobids_submitted'] = list()
        #self.top['jobid2exit'] = dict()
        self.jobids_threaded = set()
        self.jobids_submitted = set()
        self.jobid2exit = dict()

class SafeState(object):
    """Synchronized State proxy for accessing any
    data which might be modified in a Thread.
    """
    def notify_threaded(self, jobid):
        with self.lock:
            self.state.notify_threaded(jobid)
    def notify_started(self, jobid):
        with self.lock:
            self.state.notify_started(jobid)
    def notify_exited(self, jobid, rc):
        with self.lock:
            self.state.notify_exited(jobid, rc)
    def update_jobid2status(self, table):
        with self.lock:
            return self.state.update_jobid2status(table)
    def get_running_jobids(self):
        with self.lock:
            return self.state.get_running_jobids()
    def serialize(self):
        with self.lock:
            return self.state.serialize()
    def __getattr__(self, name):
        """For all other methods, just delegate.
        """
        return getattr(self.state, name)
    def __init__(self, state):
        self.state = state
        self.lock = threading.Lock()

def get_state(directory):
    """For now, we never write.
    """
    state_fn = os.path.join(directory, STATE_FN)
    if not os.path.exists(state_fn):
        return State.create(directory)
    assert False, 'No state directory needed, for now.'
    try:
        return State.deserialize(directory, open(state_fn).read())
    except Exception:
        log.exception('Failed to read state "%s". Ignoring (and soon over-writing) current state.'%state_fn)
        # TODO: Backup previous STATE_FN?
        return State(directory)
def State_save(state):
    # TODO: RW Locks, maybe for runtime of whole program.
    content = state.serialize()
    content_prev = state.content_prev
    if content == content_prev:
        return
    fn = state.get_state_fn()
    open(fn, 'w').write(content)
    log.debug('saved state to %s' %repr(os.path.abspath(fn)))
def Job_get_MetaJob(job, lang_exe=lang_bash_exe):
    return MetaJob(job, lang_exe=lang_exe)
def MetaJob_wrap(mjob, state):
    """Write wrapper contents to mjob.wrapper.
    """
    metajob_rundir = mjob.job.rundir
    wdir = metajob_rundir

    bash_template = """#!%(lang_exe)s
cmd="%(cmd)s"
rundir="%(rundir)s"
finish() {
  echo "finish code: $?"
}
trap finish 0
#printenv
echo
set -ex
while [ ! -d "$rundir" ]; do sleep 1; done
cd "$rundir"
eval "$cmd"
    """
    mji = MetaJobClass(mjob)
    wrapper_fn = os.path.join(wdir, mji.get_wrapper())
    command = mjob.job.cmd

    wrapped = bash_template %dict(
        lang_exe=mjob.lang_exe,
        cmd=command,
        rundir=metajob_rundir,
    )
    log.debug('Writing wrapper "%s"' %wrapper_fn)
    open(wrapper_fn, 'w').write(wrapped)
    st = os.stat(wrapper_fn)
    os.chmod(wrapper_fn, st.st_mode | 0o111)

class JobThread(threading.Thread):
    def run(self):
        """Propagate environment, plus env_extra.
        """
        try:
            self.notify_start(self.jobname)
            log.debug('hello! started Thread {}'.format(threading.current_thread()))
            myenv = dict(os.environ)
            myenv.update(self.env_extra)
            #log.debug('myenv:\n{}'.format(pprint.pformat(myenv)))
            log.info("Popen: '{}'".format(self.cmd))
            if not self.cmd:
                msg = 'Why is self.cmd empty? {} {} {!r}'.format(self, self.jobname, self.cmd)
                raise Exception(msg)
            p = subprocess.Popen(self.cmd, env=myenv, shell=True)
            log.debug("pid: {}".format(p.pid))
            p.wait()
            rc = p.returncode
            log.debug("rc: {}".format(rc))
            self.notify_exit(self.jobname, rc)
        except:
            log.exception('Failed to submit {}: {!r} Setting rc=42.'.format(self.jobname, self.cmd))
            self.notify_exit(self.jobname, 42)
    def __init__(self, jobname, cmd, notify_start, notify_exit, env_extra):
        super(JobThread, self).__init__()
        self.jobname = jobname
        self.cmd = cmd
        self.notify_start = notify_start
        self.notify_exit = notify_exit
        self.env_extra = env_extra

class StringJobSubmitter(object):
    """Substitute some variables into self.submission_string.
    Use mains/job_start.sh as the top script. That requires
    PYPEFLOW_JOB_START_SCRIPT in the environment as the real
    script to run. This way, we are guaranteed that the top script exists,
    and we can wait for the rest to appear in the filesystem.
    """
    def submit(self, jobname, mjob, state):
        """Prepare job (based on wrappers) and submit as a new thread.
        """
        state.set_job(jobname, mjob)
        jobname = mjob.job.jobid
        job_dict = mjob.job.options
        #nproc = mjob.job.options['NPROC']
        #mb = mjob.job.options['MB']
        mji = MetaJobClass(mjob)
        #script_fn = os.path.join(state.get_directory_wrappers(), mji.get_wrapper())
        script_fn = mji.get_wrapper()
        exe = mjob.lang_exe

        state.notify_threaded(jobname)
        self.start(jobname, state, exe, script_fn, job_dict) # Can raise
    def get_cmd(self, job_name, script_fn, job_dict):
        """Vars:
        (The old ones.) JOB_ID, STDOUT_FILE, STDERR_FILE, NPROC, MB, CMD
        """
        # We wrap in a program that waits for the executable to exist, so
        # the filesystem has time to catch up on the remote machine.
        # Hopefully, this will allow dependencies to become ready as well.
        job_start_fn = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'mains/job_start.sh')
        mapping = dict()
        stdout = script_fn + '.stdout'
        stderr = script_fn + '.stderr'
        run_dir = os.getcwd()
        mapping = dict(
                JOB_EXE='/bin/bash',
                JOB_NAME=job_name, JOB_ID=job_name,
                #JOB_OPTS=JOB_OPTS,
                #JOB_QUEUE=job_queue,
                JOB_SCRIPT=job_start_fn, CMD=job_start_fn,
                JOB_DIR=run_dir, DIR=run_dir,
                JOB_STDOUT=stdout, STDOUT_FILE=stdout,
                JOB_STDERR=stderr, STDERR_FILE=stderr,
                #MB=pypeflow_mb,
                #NPROC=pypeflow_nproc,
        )
        mapping.update(job_dict)
        if 'JOB_OPTS' in mapping:
            # a special two-level mapping: ${JOB_OPTS} is substituted first
            mapping['JOB_OPTS'] = self.sub(mapping['JOB_OPTS'], mapping)
        return self.sub(self.submission_string, mapping)
    @staticmethod
    def sub(template, mapping):
        t = string.Template(template)
        try:
            return t.substitute(mapping)
        except KeyError:
            print(repr(mapping))
            msg = 'Template substitution failed:\n template={!r}\n mapping={}'.format(
                    template, pprint.pformat(mapping))
            log.exception(msg)
            raise
    def start(self, jobname, state, exe, script_fn, job_dict):
        """Run job in thread.
        Thread will notify state.
        Can raise.
        """
        #cmd = script_fn
        cmd = self.get_cmd(jobname, script_fn, job_dict)
        # job_start.sh relies on PYPEFLOW_*
        env_extra = {
            "PYPEFLOW_JOB_START_SCRIPT": script_fn,
            "PYPEFLOW_JOB_START_TIMEOUT": "60",
        }
        log.debug('env_extra={}'.format(pprint.pformat(env_extra)))
        notify_start = state.notify_started
        notify_exit = state.notify_exited
        th = JobThread(jobname, cmd, notify_start, notify_exit, env_extra)
        #th.setDaemon(True)
        th.start()
    def __repr__(self):
        return 'StringJobSubmitter(%s)' %repr(self.submission_string)
    def __init__(self, submission_string):
        self.submission_string = submission_string

def link_rundir(state_rundir, user_rundir):
    if user_rundir:
        link_fn = os.path.join(user_rundir, 'pwatcher.dir')
        if os.path.lexists(link_fn):
            os.unlink(link_fn)
        os.symlink(os.path.abspath(state_rundir), link_fn)

def cmd_run(state, jobids, job_type, job_dict):
    """
    Wrap them and run them locally, each in the foreground of a thread.
    """
    jobs = dict()
    submitted = list()
    result = {'submitted': submitted}
    if job_type != 'string':
        log.debug("NOTE: In blocking pwatcher, job_type={!r}, should be 'string'".format(job_type))
    for jobid, desc in jobids.items():
        assert 'cmd' in desc
        cmd = desc['cmd']
        if 'rundir' in desc:
            rundir = desc['rundir']
        else:
            rundir = os.path.dirname(cmd)
        # These are all required now.
        #nproc = desc['job_nproc']
        #mb = desc['job_mb']
        local = int(desc['job_local'])
        options = copy.deepcopy(desc['job_dict']) #dict(NPROC=nproc, MB=mb, local=local)
        options['local'] = local
        jobs[jobid] = Job(jobid, cmd, rundir, options)
    log.debug('jobs:\n%s' %pprint.pformat(jobs))
    submission_string = job_dict['submit']
    basic_submitter = StringJobSubmitter(submission_string)
    local_submitter = StringJobSubmitter(LOCAL_SUBMISSION_STRING)
    log.debug('Basic submitter: {!r}'.format(basic_submitter))
    for jobid, job in jobs.items():
        #desc = jobids[jobid]
        log.debug(' starting job %s' %pprint.pformat(job))
        mjob = Job_get_MetaJob(job)
        MetaJob_wrap(mjob, state)
        try:
            #link_rundir(state.get_directory_job(jobid), desc.get('rundir'))
            if job.options['local']:
                submitter = local_submitter
            else:
                submitter = basic_submitter
                if not submission_string:
                    raise Exception('No "submit" key in job_dict:{!r}.'.format(job_dict))
            submitter.submit(jobid, mjob, state)
            submitted.append(jobid)
        except Exception:
            raise
            log.exception('Failed to submit background-job:\n{!r}'.format(
                submitter))
    return result
    # The caller is responsible for deciding what to do about job-submission failures. Re-try, maybe?

def system(call, checked=False):
    log.info('!{}'.format(call))
    rc = os.system(call)
    if checked and rc:
        raise Exception('{} <- {!r}'.format(rc, call))
    return rc

_warned = dict()
def warnonce(hashkey, msg):
    if hashkey in _warned:
        return
    log.warning(msg)
    _warned[hashkey] = True

def cmd_query(state, which, jobids):
    """Return the state of named jobids.
    If which=='list', then query jobs listed as jobids.
    If which=='known', then query all known jobs.
    If which=='infer', same as 'known' now.
    """
    result = dict()
    jobstats = dict()
    result['jobids'] = jobstats
    if which == 'list':
        for jobid in jobids:
            jobstats[jobid] = 'UNKNOWN'
    state.update_jobid2status(jobstats)
    jobids = set(jobids)
    if which == 'list':
        for jobid in list(jobstats.keys()):
            # TODO: This might remove thousands. We should pass jobids along to update_jobid2status().
            if jobid not in jobids:
                del jobstats[jobid]
    return result
def cmd_delete(state, which, jobids):
    """Kill designated jobs, including (hopefully) their
    entire process groups.
    If which=='list', then kill all jobs listed as jobids.
    If which=='known', then kill all known jobs.
    If which=='infer', then kill all jobs with heartbeats.
    """
    log.error('Noop. We cannot kill blocked threads. Hopefully, everything will die on SIGTERM.')
def makedirs(path):
    if not os.path.isdir(path):
        os.makedirs(path)
def readjson(ifs):
    """Del keys that start with ~.
    That lets us have trailing commas on all other lines.
    """
    content = ifs.read()
    log.debug('content:%s' %repr(content))
    jsonval = json.loads(content)
    #pprint.pprint(jsonval)
    def striptildes(subd):
        if not isinstance(subd, dict):
            return
        for k,v in list(subd.items()):
            if k.startswith('~'):
                del subd[k]
            else:
                striptildes(v)
    striptildes(jsonval)
    #pprint.pprint(jsonval)
    return jsonval

class ProcessWatcher(object):
    def run(self, jobids, job_type, job_defaults_dict):
        #import traceback; log.debug(''.join(traceback.format_stack()))
        log.debug('run(jobids={}, job_type={}, job_defaults_dict={})'.format(
            '<%s>'%len(jobids), job_type, job_defaults_dict))
        return cmd_run(self.state, jobids, job_type, job_defaults_dict)
    def query(self, which='list', jobids=[]):
        log.debug('query(which={!r}, jobids={})'.format(
            which, '<%s>'%len(jobids)))
        return cmd_query(self.state, which, jobids)
    def delete(self, which='list', jobids=[]):
        log.debug('delete(which={!r}, jobids={})'.format(
            which, '<%s>'%len(jobids)))
        return cmd_delete(self.state, which, jobids)
    def __init__(self, state):
        # state must be thread-safe
        self.state = state

def get_process_watcher(directory):
    state = get_state(directory)
    state = SafeState(state) # thread-safe proxy
    #log.debug('state =\n%s' %pprint.pformat(state.top))
    return ProcessWatcher(state)
    #State_save(state)

@contextlib.contextmanager
def process_watcher(directory):
    """This will (someday) hold a lock, so that
    the State can be written safely at the end.
    """
    state = get_state(directory)
    state = SafeState(state) # thread-safe proxy
    #log.debug('state =\n%s' %pprint.pformat(state.top))
    yield ProcessWatcher(state)
    #State_save(state)

def main(prog, cmd, state_dir='mainpwatcher', argsfile=None):
    logging.basicConfig()
    logging.getLogger().setLevel(logging.NOTSET)
    log.warning('logging basically configured')
    log.debug('debug mode on')
    assert cmd in ['run', 'query', 'delete']
    ifs = sys.stdin if not argsfile else open(argsfile)
    argsdict = readjson(ifs)
    log.info('argsdict =\n%s' %pprint.pformat(argsdict))
    with process_watcher(state_dir) as watcher:
        result = getattr(watcher, cmd)(**argsdict)
        if result is not None:
            log.info('getattr({!r}, {!r}): {}'.format(
                watcher, cmd, pprint.pformat(result)))
        log.info('Waiting for running jobs...r')
        while watcher.state.get_running_jobids():
            log.info('running: {!s}'.format(watcher.state.get_running_jobids()))
            time.sleep(1)

if __name__ == "__main__":
    #import pdb
    #pdb.set_trace()
    main(*sys.argv) # pylint: disable=no-value-for-parameter
