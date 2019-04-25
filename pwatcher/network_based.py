#!/usr/bin/env python2.7
"""Network-based process-watcher.

This is meant to be part of a 2-process system. For now, let's call these
processes the Definer and the Watcher.
* The Definer creates a graph of tasks and starts a resolver loop, like
  pypeflow. It keeps a Waiting list, a Running list, and a Done list. It then
  communicates with the Watcher.
* The Watcher has 3 basic functions in its API.
  1. Spawn jobs.
  2. Kill jobs.
  3. Query jobs.
1. Spawning jobs
   The job definition includes the script, how to run it (locally, qsub, etc.),
   and maybe some details (unique-id, run-directory). The Watcher then:
     * wraps the script within something to update the heartbeat server
       periodically,
     * spawns each job (possibly as a background process locally),
     * and records info (including PID or qsub-name) in a persistent database.
2. Kill jobs.
   Since it has a persistent database, it can always kill any job, upon request.
3. Query jobs.
   Whenever requested, it can poll the server for all or any jobs, returning the
   subset of completed jobs.

The Definer would call the Watcher to spawn tasks, and then periodically to poll
them. Because these are both now single-threaded, the Watcher *could* be a
function within the Definer, or a it could be blocking call to a separate
process.

Caching/timestamp-checking would be done in the Definer, flexibly specific to
each Task.

Eventually, the Watcher could be in a different programming language. Maybe
perl. (In bash, a background heartbeat gets is own process group, so it can be
hard to clean up.)
"""

try:
    from shlex import quote
except ImportError:
    from pipes import quote
import socketserver
import collections
import contextlib
import glob
import json
import logging
import os
import pprint
import re
import shutil
import signal
import socket
import subprocess
import sys
import multiprocessing
import time
import traceback

log = logging.getLogger(__name__)

HEARTBEAT_RATE_S = 600
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

# send message delimited with a \0
def socket_send(socket, message):
    socket.sendall(b'{}\0'.format(message))

# receive all of \0 delimited message
# may discard content past \0, if any, so not safe to call twice on same socket
def socket_read(socket):
    buffer = bytearray(b' ' * 256)
    nbytes = socket.recv_into(buffer, 256)
    if nbytes == 0:		# empty message
        return
    message = ''
    while nbytes != 0:
        try:	# index() raises when it can't find the character
            i = buffer[:nbytes].index('\0')
            message += str(buffer[:i])	# discard past delimiter
            break
        except ValueError:	# haven't reached end yet
            message += str(buffer)
        nbytes = socket.recv_into(buffer, 256)
    return message

# TODO: have state be persistent in some fashion, so we can be killed
# and restarted
class StatusServer(socketserver.BaseRequestHandler):
    """input format is "command [jobid [arg1 [arg2]]]"
    job status update commands are:
        i <jobid> <pid> <pgid>	- initialize job
        d <jobid>		- delete job info
        e <jobid> <exit_status>	- job exit
        h <jobid>		- heartbeat
        s <jobid> <stdout/stderr output> - record script output
    job status query commands are:
        A         - returns authentication key
        D         - returns the entire server state
        L         - returns a space-delimited list of all jobids
        P <jobid> - returns "pid pgid" if available
        Q <jobid> - for done/exited jobs returns "EXIT <exit_status>"
                  - for others, returns "RUNNING <seconds_since_last_update>"
    """
    def handle(self):
        message = socket_read(self.request)
        if not message:
            return
        args = message.split(None, 4)
        if not args:		# make sure we have some arguments
            return
        command = args[0]
        if command == 'A':	# send authentication key
            # need repr() as it's a byte string and may have zeroes
            socket_send(self.request, repr(multiprocessing.Process().authkey))
            return
        if command == 'D':	# send server state
            socket_send(self.request, repr(self.server.server_job_list))
            return
        if command == 'L':	# send list of jobids
            socket_send(self.request, ' '.join(self.server.server_job_list.keys()))
            return
        if len(args) < 2:	# make sure we have a jobid argument
            return
        jobid = args[1]
        if command == 'i':	# job start (with pid and pgid)
            if len(args) < 4:	# missing pid/pgid
                self.server.server_job_list[jobid] = [time.time(), None, None, None]
            else:
                self.server.server_job_list[jobid] = [time.time(), args[2], args[3], None]
                with open(os.path.join(self.server.server_pid_dir, jobid), 'w') as f:
                    f.write('{} {}'.format(args[2], args[3]))
        elif command == 'h' or command == 'e' or command == 's': # updates
            log_file = os.path.join(self.server.server_log_dir, jobid)
            if jobid in self.server.server_job_list:
                j = self.server.server_job_list[jobid]
                j[0] = time.time()
            else:	# unknown jobid, so create job
                j = self.server.server_job_list[jobid] = [time.time(), None, None, None]
                with open(log_file, 'w'):
                    pass
            os.utime(log_file, (j[0], j[0]))
            if command == 'e':		# exit (with exit code)
                if len(args) < 3:	# lack of error code is an error (bug)
                    j[3] = '99'
                else:
                    j[3] = args[2]
                with open(os.path.join(self.server.server_exitrc_dir, jobid), 'w') as f:
                    f.write(str(j[3]))
            elif command == 's':	# record log output
                if len(args) > 2:
                    with open(log_file, 'a') as f:
                        f.writelines(' '.join(args[2:]))
        elif jobid not in self.server.server_job_list:	# unknown jobid
            return
        elif command == 'd':		# delete job info
            del self.server.server_job_list[jobid]
        elif command == 'Q' or command == 'P':			# queries
            j = self.server.server_job_list[jobid]
            if command == 'Q':		# query job status
                if j[3]:		# has exit status
                    socket_send(self.request, 'EXIT {}'.format(j[3]))
                else:			# return time since last heartbeat
                    # convert to int to keep message shorter -
                    # we don't need the extra precision, anyway
                    diff = int(time.time() - j[0])
                    if diff < 0:	# possible with a clock change
                        diff = 0
                    socket_send(self.request, 'RUNNING {}'.format(diff))
            elif command == 'P':	# get job pid and pgid
                if j[1] != None and j[2] != None:
                    socket_send(self.request, '{} {}'.format(j[1], j[2]))

# get local ip address while avoiding some common problems
def get_localhost_ipaddress(hostname, port):
    if hostname == '0.0.0.0':
        # use getaddrinfo to work with ipv6
        list = socket.getaddrinfo(socket.gethostname(), port, socket.AF_INET, socket.SOCK_STREAM)
        for a in list:
            # TODO: have this part work with ipv6 addresses
            if len(a[4]) == 2 and a[4][0] != '127.0.0.1':
                return a[4][0]
    return hostname

# if we end up restarting a partially killed process, we'll try
# to pick up the ongoing heartbeats
class ReuseAddrServer(socketserver.TCPServer):
    def restore_from_directories(self):
        """
        as our heartbeat server has been down, there's no accurate way
        to set the heartbeat times, so we just use the current time, and
        hope for an update from the process before the heartbeat timeout
        """
        if os.path.isdir(self.server_pid_dir):
            for file in os.listdir(self.server_pid_dir):
                with open(os.path.join(self.server_pid_dir, file)) as f:
                    (pid, pgid) = f.readline().strip().split()
                if pid and pgid:
                    self.server_job_list[file] = [time.time(), pid, pgid, None]
        # for finished proceses, though, we use the exit file time
        if os.path.isdir(self.server_exitrc_dir):
            for file in os.listdir(self.server_exitrc_dir):
                fn = os.path.join(self.server_exitrc_dir, file)
                with open(fn) as f:
                    rc = f.readline().strip()
                if rc:
                    if file in self.server_job_list:
                        self.server_job_list[file][0] = os.path.getmtime(fn)
                        self.server_job_list[file][3] = rc
                    else:
                        self.server_job_list[file] = [os.path.getmtime(fn), None, None, rc]
    def __init__(self, server_address, RequestHandlerClass, server_directories):
        self.allow_reuse_address = True
        self.server_log_dir, self.server_pid_dir, self.server_exitrc_dir = server_directories
        # {jobid} = [pid, pgid, heartbeat timestamp, exit rc]
        self.server_job_list = dict()
        self.restore_from_directories()
        socketserver.TCPServer.__init__(self, server_address, RequestHandlerClass)

def start_server(server_directories, hostname='', port=0):
    server = ReuseAddrServer((hostname, port), StatusServer, server_directories)
    hostname, port = server.socket.getsockname()
    hostname = get_localhost_ipaddress(hostname, port)
    # we call it a thread, but use a process to avoid the gil
    hb_thread = multiprocessing.Process(target=server.serve_forever)
    # set daemon to make sure server shuts down when main program finishes
    hb_thread.daemon = True
    hb_thread.start()
    log.debug('server ({}, {}) alive? {}'.format(hostname, port, hb_thread.is_alive()))
    return (hb_thread.authkey, (hostname, port))

class MetaJobClass(object):
    ext = {
        lang_python_exe: '.py',
        lang_bash_exe: '.bash',
    }
    def get_wrapper(self):
        return 'run-%s%s' %(self.mj.job.jobid, self.ext[self.mj.lang_exe])
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
    def get_state_fn(self):
        return os.path.join(self.__directory, STATE_FN)
    def get_heartbeat_server(self):
        return self.top['server']
    def get_directory_wrappers(self):
        return os.path.join(self.__directory, 'wrappers')
    def get_directory_exits(self):	# more like, emergency exits ;)
        return 'exits'
    def get_server_directories(self):
        return (os.path.join(self.__directory, 'log'), os.path.join(self.__directory, 'pid'), os.path.join(self.__directory, 'exit'))
    def submit_background(self, bjob):
        """Run job in background.
        Record in state.
        """
        jobid = bjob.mjob.job.jobid
        self.top['jobs'][jobid] = bjob
        mji = MetaJobClass(bjob.mjob)
        script_fn = os.path.join(self.get_directory_wrappers(), mji.get_wrapper())
        exe = bjob.mjob.lang_exe
        with cd(bjob.mjob.job.rundir):
            bjob.submit(self, exe, script_fn) # Can raise
        log.info('Submitted backgroundjob=%s'%repr(bjob))
        self.top['jobids_submitted'].append(jobid)
        self.__changed = True
    def get_mji(self, jobid):
        mjob = self.top['jobs'][jobid].mjob
        return MetaJobClass(mjob)
    def get_bjob(self, jobid):
        return self.top['jobs'][jobid]
    def get_bjobs(self):
        return self.top['jobs']
    def get_mjobs(self):
        return {jobid: bjob.mjob for jobid, bjob in self.top['jobs'].items()}
    def add_deleted_jobid(self, jobid):
        self.top['jobids_deleted'].append(jobid)
        self.__changed = True
    def restart_server(self):
        hsocket = socket.socket()
        try:		# see if our old server is still there
            hsocket.settimeout(60)
            hsocket.connect(self.get_heartbeat_server())
            socket_send(hsocket, 'A')
            line = socket_read(hsocket)
            hsocket.close()
            # auth is binary, so we have to eval to restore it
            if self.top['auth'] != eval(line):	# not our server
                raise IOError()
        except IOError:	# need to restart server
            try:	# first we try restarting at same location
                old_hostname, old_port = self.get_heartbeat_server()
                self.top['auth'], self.top['server'] = start_server(self.get_server_directories(), old_hostname, old_port)
            except Exception:
                log.exception('Failed to restore previous watcher server settings')
                try:	# next we try restarting with original arguments
                    old_hostname, old_port = self.top['server_args']
                    self.top['auth'], self.top['server'] = start_server(self.get_server_directories(), old_hostname, old_port)
                except Exception:
                    self.top['auth'], self.top['server'] = start_server(self.get_server_directories())
                self.__changed = True
    # if we restarted, orphaned jobs might have left exit files
    # update the server with exit info
    def cleanup_exits(self):
        if os.path.exists(self.get_directory_exits()):
            for file in os.listdir(self.get_directory_exits()):
                exit_fn = os.path.join(self.get_directory_exits(), file)
                with open(exit_fn) as f:
                    rc = f.readline().strip()
                hsocket = socket.socket()
                hsocket.connect(self.get_heartbeat_server())
                #socket_send(hsocket, 'e {} {}'.format(jobid, rc)) #TODO: Must get jobid from somewhere
                hsocket.close()
                os.remove(exit_fn)
        else:
            makedirs(self.get_directory_exits())
    def restore_from_save(self, state_fn):
        with open(state_fn) as f:
            self.top = eval(f.read())
        self.restart_server()
        self.cleanup_exits()
        self.__initialized = True
    def initialize(self, directory, hostname='', port=0):
        self.__directory = os.path.abspath(directory)
        state_fn = self.get_state_fn()
        if os.path.exists(state_fn):
            try:			# try to restore state from last save
                if self.restore_from_save(state_fn):
                    return
            except Exception:
                log.exception('Failed to use saved state "%s". Ignoring (and soon over-writing) current state.'%state_fn)
        makedirs(self.get_directory_wrappers())
        makedirs(self.get_directory_exits())
        for i in self.get_server_directories():
            makedirs(i)
        if directory != 'mypwatcher':	# link mypwatcher to dir
            if os.path.exists('mypwatcher'):
                try:
                    if os.path.islink('mypwatcher'):
                        os.remove('mypwatcher')
                    else:
                        shutil.rmtree('mypwatcher')
                except OSError:
                    pass
            os.symlink(self.__directory, 'mypwatcher')
        self.top['server_args'] = (hostname, port)
        self.top['auth'], self.top['server'] = start_server(self.get_server_directories(), hostname, port)
        self.__initialized = True
        self.__changed = True
    def is_initialized(self):
        return self.__initialized
    def save(self):
        # TODO: RW Locks, maybe for runtime of whole program.
        # only save if there has been a change
        if self.__changed:
            content = pprint.pformat(self.top)
            fn = self.get_state_fn()
            with open(fn + '.tmp', 'w') as f:
                f.write(content)
            # as atomic as we can portably manage
            shutil.move(fn + '.tmp', fn)
            self.__changed = False
            log.debug('saved state to %s' %repr(os.path.abspath(fn)))
    def __init__(self):
        self.__initialized = False
        self.__changed = False
        self.top = dict()
        self.top['jobs'] = dict()
        self.top['jobids_deleted'] = list()
        self.top['jobids_submitted'] = list()

watcher_state = State()

def get_state(directory, hostname='', port=0):
    if not watcher_state.is_initialized():
        watcher_state.initialize(directory, hostname, port)
    return watcher_state
def Job_get_MetaJob(job, lang_exe=lang_bash_exe):
    return MetaJob(job, lang_exe=lang_exe)
def MetaJob_wrap(mjob, state):
    """Write wrapper contents to mjob.wrapper.
    """
    wdir = state.get_directory_wrappers()
    # the wrapped job may chdir(), so use abspath
    edir = os.path.abspath(state.get_directory_exits())
    metajob_rundir = mjob.job.rundir

    bash_template = """#!%(lang_exe)s
%(cmd)s
    """
    # We do not bother with 'set -e' here because this script is run either
    # in the background or via qsub.
    templates = {
        lang_python_exe: python_template,
        lang_bash_exe: bash_template,
    }
    mji = MetaJobClass(mjob)
    wrapper_fn = os.path.join(wdir, mji.get_wrapper())
    heartbeat_server, heartbeat_port = state.get_heartbeat_server()
    rate = HEARTBEAT_RATE_S
    command = mjob.job.cmd
    jobid = mjob.job.jobid
    exit_sentinel_fn = os.path.join(edir, jobid)

    prog = 'python2.7 -m pwatcher.mains.network_heartbeat'
    heartbeat_wrapper_template = "{prog} --directory={metajob_rundir} --heartbeat-server={heartbeat_server} --heartbeat-port={heartbeat_port} --exit-dir={edir} --rate={rate} --jobid={jobid} {command} || echo 99 >| {exit_sentinel_fn}"
    # We write 99 into exit-sentinel if the wrapper fails.
    wrapped = heartbeat_wrapper_template.format(**locals())
    log.debug('Wrapped "%s"' %wrapped)

    wrapped = templates[mjob.lang_exe] %dict(
        lang_exe=mjob.lang_exe,
        cmd=wrapped,
    )
    log.debug('Writing wrapper "%s"' %wrapper_fn)
    with open(wrapper_fn, 'w') as f:
        f.write(wrapped)

def background(script, exe='/bin/bash'):
    """Start script in background (so it keeps going when we exit).
    Run in cwd.
    For now, stdout/stderr are captured.
    Return pid.
    """
    args = [exe, script]
    sin = open(os.devnull)
    sout = open(os.devnull, 'w')
    serr = open(os.devnull, 'w')
    pseudo_call = '{exe} {script} 1>|stdout 2>|stderr & '.format(exe=exe, script=script)
    log.debug('dir: {!r}\ncall: {!r}'.format(os.getcwd(), pseudo_call))
    proc = subprocess.Popen([exe, script], stdin=sin, stdout=sout, stderr=serr)
    pid = proc.pid
    log.debug('pid=%s pgid=%s sub-pid=%s' %(os.getpid(), os.getpgid(0), proc.pid))
    #checkcall = 'ls -l /proc/{}/cwd'.format(
    #        proc.pid)
    #system(checkcall, checked=True)
    return pid

def qstripped(option):
    """Given a string of options, remove any -q foo.

    >>> qstripped('-xy -q foo -z bar')
    '-xy -z bar'
    """
    # For now, do not strip -qfoo
    vals = option.strip().split()
    while '-q' in vals:
        i = vals.index('-q')
        vals = vals[0:i] + vals[i+2:]
    return ' '.join(vals)

class MetaJobLocal(object):
    def submit(self, state, exe, script_fn):
        """Can raise.
        """
        #sge_option = self.mjob.job.options['sge_option']
        #assert sge_option is None, sge_option # might be set anyway
        pid = background(script_fn, exe=self.mjob.lang_exe)
    def kill(self, state):
        """Can raise.
        """
        hsocket = socket.socket()
        try:
            hsocket.connect(state.get_heartbeat_server())
            socket_send(hsocket, 'P {}'.format(self.mjob.job.jobid))
            line = socket_read(hsocket)
            hsocket.close()
        except IOError as e:
            log.exception('Failed to get pig/pgid for {}: {!r}'.format(self.mjob.job.jobid, e))
            return
        args = line.split(None, 2)
        pid = int(args[0])
        pgid = int(args[1])
        sig = signal.SIGKILL
        log.info('Sending signal(%s) to pgid=-%s (pid=%s) based on heartbeat server' %(sig, pgid, pid))
        try:
            os.kill(-pgid, sig)
        except Exception:
            log.exception('Failed to kill(%s) pgid=-%s for %r. Trying pid=%s' %(sig, pgid, self.mjob.job.jobid, pid))
            os.kill(pid, sig)
    def __repr__(self):
        return 'MetaJobLocal(%s)' %repr(self.mjob)
    def __init__(self, mjob):
        self.mjob = mjob # PUBLIC
class MetaJobSge(object):
    def submit(self, state, exe, script_fn):
        """Can raise.
        """
        specific = self.specific
        #cwd = os.getcwd()
        job_name = self.get_jobname()
        sge_option = qstripped(self.mjob.job.options['sge_option'])
        job_queue = self.mjob.job.options['job_queue']
        # Add shebang, in case shell_start_mode=unix_behavior.
        #   https://github.com/PacificBiosciences/FALCON/pull/348
        with open(script_fn, 'r') as original: data = original.read()
        with open(script_fn, 'w') as modified: modified.write("#!/bin/bash" + "\n" + data)
        sge_cmd = 'qsub -N {job_name} -q {job_queue} {sge_option} {specific} -cwd -o stdout -e stderr -S {exe} {script_fn}'.format(
                **locals())
        system(sge_cmd, checked=True) # TODO: Capture q-jobid
    def kill(self, state, heartbeat):
        """Can raise.
        """
        job_name = self.get_jobname()
        sge_cmd = 'qdel {}'.format(
                job_name)
        system(sge_cmd, checked=False)
    def get_jobname(self):
        """Some systems are limited to 15 characters, so for now we simply truncate the jobid.
        TODO: Choose a sequential jobname and record it. Priority: low, since collisions are very unlikely.
        """
        return self.mjob.job.jobid[:15]
    def __repr__(self):
        return 'MetaJobSge(%s)' %repr(self.mjob)
    def __init__(self, mjob):
        self.mjob = mjob
        self.specific = '-V' # pass enV; '-j y' => combine out/err
class MetaJobPbs(object):
    """
usage: qsub [-a date_time] [-A account_string] [-c interval]
        [-C directive_prefix] [-e path] [-h ] [-I [-X]] [-j oe|eo] [-J X-Y[:Z]]
        [-k o|e|oe] [-l resource_list] [-m mail_options] [-M user_list]
        [-N jobname] [-o path] [-p priority] [-q queue] [-r y|n]
        [-S path] [-u user_list] [-W otherattributes=value...]
        [-v variable_list] [-V ] [-z] [script | -- command [arg1 ...]]
    """
    def submit(self, state, exe, script_fn):
        """Can raise.
        """
        specific = self.specific
        #cwd = os.getcwd()
        job_name = self.get_jobname()
        sge_option = qstripped(self.mjob.job.options['sge_option'])
        job_queue = self.mjob.job.options['job_queue']
        # Add shebang, in case shell_start_mode=unix_behavior.
        #   https://github.com/PacificBiosciences/FALCON/pull/348
        with open(script_fn, 'r') as original: data = original.read()
        with open(script_fn, 'w') as modified: modified.write("#!/bin/bash" + "\n" + data)
        sge_cmd = 'qsub -N {job_name} -q {job_queue} {sge_option} {specific} -o ev/null -e /dev/null -S {exe} {script_fn}'.format(
                **locals())
        system(sge_cmd, checked=True) # TODO: Capture q-jobid
    def kill(self, state, heartbeat):
        """Can raise.
        """
        job_name = self.get_jobname()
        sge_cmd = 'qdel {}'.format(
                job_name)
        system(sge_cmd, checked=False)
    def get_jobname(self):
        """Some systems are limited to 15 characters, so for now we simply truncate the jobid.
        TODO: Choose a sequential jobname and record it. Priority: low, since collisions are very unlikely.
        """
        return self.mjob.job.jobid[:15]
    def __repr__(self):
        return 'MetaJobPbs(%s)' %repr(self.mjob)
    def __init__(self, mjob):
        self.mjob = mjob
        self.specific = '-V' # pass enV; '-j y' => combine out/err
class MetaJobTorque(object):
    # http://docs.adaptivecomputing.com/torque/4-0-2/help.htm#topics/commands/qsub.htm
    def submit(self, state, exe, script_fn):
        """Can raise.
        """
        specific = self.specific
        #cwd = os.getcwd()
        job_name = self.get_jobname()
        sge_option = qstripped(self.mjob.job.options['sge_option'])
        job_queue = self.mjob.job.options['job_queue']
        cwd = os.getcwd()
        # Add shebang, in case shell_start_mode=unix_behavior.
        #   https://github.com/PacificBiosciences/FALCON/pull/348
        with open(script_fn, 'r') as original: data = original.read()
        with open(script_fn, 'w') as modified: modified.write("#!/bin/bash" + "\n" + data)
        sge_cmd = 'qsub -N {job_name} -q {job_queue} {sge_option} {specific} -d {cwd} -o stdout -e stderr -S {exe} {script_fn}'.format(
                **locals())
        system(sge_cmd, checked=True) # TODO: Capture q-jobid
    def kill(self, state, heartbeat):
        """Can raise.
        """
        job_name = self.get_jobname()
        sge_cmd = 'qdel {}'.format(
                job_name)
        system(sge_cmd, checked=False)
    def get_jobname(self):
        """Some systems are limited to 15 characters, so for now we simply truncate the jobid.
        TODO: Choose a sequential jobname and record it. Priority: low, since collisions are very unlikely.
        """
        return self.mjob.job.jobid[:15]
    def __repr__(self):
        return 'MetaJobTorque(%s)' %repr(self.mjob)
    def __init__(self, mjob):
        super(MetaJobTorque, self).__init__(mjob)
        self.specific = '-V' # pass enV; '-j oe' => combine out/err
        self.mjob = mjob
class MetaJobSlurm(object):
    def submit(self, state, exe, script_fn):
        """Can raise.
        """
        job_name = self.get_jobname()
        sge_option = qstripped(self.mjob.job.options['sge_option'])
        job_queue = self.mjob.job.options['job_queue']
        cwd = os.getcwd()
        sge_cmd = 'sbatch -J {job_name} -p {job_queue} {sge_option} -D {cwd} -o stdout -e stderr --wrap="{exe} {script_fn}"'.format(
                **locals())
        # "By default all environment variables are propagated."
        #  http://slurm.schedmd.com/sbatch.html
        system(sge_cmd, checked=True) # TODO: Capture sbatch-jobid
    def kill(self, state, heartbeat):
        """Can raise.
        """
        job_name = self.get_jobname()
        sge_cmd = 'scancel -n {}'.format(
                job_name)
        system(sge_cmd, checked=False)
    def get_jobname(self):
        """Some systems are limited to 15 characters, so for now we simply truncate the jobid.
        TODO: Choose a sequential jobname and record it. Priority: low, since collisions are very unlikely.
        """
        return self.mjob.job.jobid[:15]
    def __repr__(self):
        return 'MetaJobSlurm(%s)' %repr(self.mjob)
    def __init__(self, mjob):
        self.mjob = mjob
class MetaJobLsf(object):
    def submit(self, state, exe, script_fn):
        """Can raise.
        """
        job_name = self.get_jobname()
        sge_option = qstripped(self.mjob.job.options['sge_option'])
        job_queue = self.mjob.job.options['job_queue']
        sge_cmd = 'bsub -J {job_name} -q {job_queue} {sge_option} -o stdout -e stderr "{exe} {script_fn}"'.format(
                **locals())
        # "Sets the user's execution environment for the job, including the current working directory, file creation mask, and all environment variables, and sets LSF environment variables before starting the job."
        system(sge_cmd, checked=True) # TODO: Capture q-jobid
    def kill(self, state, heartbeat):
        """Can raise.
        """
        job_name = self.get_jobname()
        sge_cmd = 'bkill -J {}'.format(
                job_name)
        system(sge_cmd, checked=False)
    def get_jobname(self):
        """Some systems are limited to 15 characters, so for now we simply truncate the jobid.
        TODO: Choose a sequential jobname and record it. Priority: low, since collisions are very unlikely.
        """
        return self.mjob.job.jobid[:15]
    def __repr__(self):
        return 'MetaJobLsf(%s)' %repr(self.mjob)
    def __init__(self, mjob):
        self.mjob = mjob

def cmd_run(state, jobids, job_type, job_queue):
    """On stdin, each line is a unique job-id, followed by run-dir, followed by command+args.
    Wrap them and run them locally, in the background.
    """
    jobs = dict()
    submitted = list()
    result = {'submitted': submitted}
    for jobid, desc in jobids.items():
        assert 'cmd' in desc
        options = {}
        if 'job_queue' not in desc:
            raise Exception(pprint.pformat(desc))
        for k in ('sge_option', 'job_type', 'job_queue'): # extras to be stored
            if k in desc:
                if desc[k]:
                    options[k] = desc[k]
        if options.get('sge_option', None) is None:
            # This way we can always safely include it.
            options['sge_option'] = ''
        if not options.get('job_queue'):
            options['job_queue'] = job_queue
        if not options.get('job_type'):
            options['job_type'] = job_type
        jobs[jobid] = Job(jobid, desc['cmd'], desc['rundir'], options)
    log.debug('jobs:\n%s' %pprint.pformat(jobs))
    for jobid, job in jobs.items():
        desc = jobids[jobid]
        log.info('starting job %s' %pprint.pformat(job))
        mjob = Job_get_MetaJob(job)
        MetaJob_wrap(mjob, state)
        options = job.options
        my_job_type = desc.get('job_type')
        if my_job_type is None:
            my_job_type = job_type
        my_job_type = my_job_type.upper()
        if my_job_type != 'LOCAL':
            if ' ' in job_queue:
                msg = 'For pwatcher=network_based, job_queue cannot contain spaces:\n job_queue={!r}\n job_type={!r}'.format(
                    job_queue, my_job_type)
                raise Exception(msg)
        if my_job_type == 'LOCAL':
            bjob = MetaJobLocal(mjob)
        elif my_job_type == 'SGE':
            bjob = MetaJobSge(mjob)
        elif my_job_type == 'PBS':
            bjob = MetaJobPbs(mjob)
        elif my_job_type == 'TORQUE':
            bjob = MetaJobTorque(mjob)
        elif my_job_type == 'SLURM':
            bjob = MetaJobSlurm(mjob)
        elif my_job_type == 'LSF':
            bjob = MetaJobLsf(mjob)
        else:
            raise Exception('Unknown my_job_type=%s' %repr(my_job_type))
        try:
            state.submit_background(bjob)
            submitted.append(jobid)
        except Exception:
            log.exception('In pwatcher.network_based.cmd_run(), failed to submit background-job:\n{!r}'.format(
                bjob))
    return result
    # The caller is responsible for deciding what to do about job-submission failures. Re-try, maybe?

def system(call, checked=False):
    log.info('!{}'.format(call))
    rc = os.system(call)
    if checked and rc:
        raise Exception('{} <- {!r}'.format(rc, call))

_warned = dict()
def warnonce(hashkey, msg):
    if hashkey in _warned:
        return
    log.warning(msg)
    _warned[hashkey] = True

def get_status(state, jobid):
    hsocket = socket.socket()
    try:
        hsocket.connect(state.get_heartbeat_server())
        socket_send(hsocket, 'Q {}'.format(jobid))
        line = socket_read(hsocket)
        hsocket.close()
    except IOError:	# server can't be reached for comment
        return 'UNKNOWN'
    if not line:	# bad formatting
        return 'UNKNOWN'
    args = line.split(None, 2)	# status, arg
    if len(args) != 2:
        return 'UNKNOWN'
    if args[0] == 'EXIT':
        return line
    elif args[0] == 'RUNNING':
        if 3*HEARTBEAT_RATE_S < int(args[1]):
            msg = 'DEAD job? 3*{} < {} for {!r}'.format(
                HEARTBEAT_RATE_S, args[1], jobid)
            log.debug(msg)
            warnonce(jobid, msg)
            return 'DEAD'
        else:
            return 'RUNNING'
    return 'UNKNOWN'
def cmd_query(state, which, jobids):
    """Return the state of named jobids.
    """
    result = dict()
    jobstats = dict()
    result['jobids'] = jobstats
    for jobid in find_jobids(state, which, jobids):
        status = get_status(state, jobid)
        log.debug('Status %s for jobid:%s' %(status, jobid))
        jobstats[jobid] = status
    return result
def get_jobid2pid(pid2mjob):
    result = dict()
    for pid, mjob in pid2mjob.items():
        jobid = mjob.job.jobid
        result[jobid] = pid
    return result
def find_jobids(state, which, jobids):
    """Yield jobids.
    If which=='list', then query jobs listed as jobids.
    If which=='known', then query all known jobs.
    If which=='infer', then query all jobs with heartbeats.
    These are not quite finished, but already useful.
    """
    #log.debug('find_jobids for which=%s, jobids=%s' %(which, pprint.pformat(jobids)))
    if which == 'infer':
        hsocket = socket.socket()
        try:
            hsocket.connect(state.get_heartbeat_server())
            socket_send(hsocket, 'L')
            line = socket_read(hsocket)
            hsocket.close()
        except IOError as e:
            log.exception('In find_jobids(), unable to infer jobid list: {!r}'.format(e))
            yield
        for jobid in line.split():
            yield jobid
    elif which == 'known':
        jobid2mjob = state.get_mjobs()
        for jobid, mjob in jobid2mjob.items():
            yield jobid
    elif which == 'list':
        #jobid2mjob = state.get_mjobs()
        #log.debug('jobid2mjob:\n%s' %pprint.pformat(jobid2mjob))
        for jobid in jobids:
            #log.debug('jobid=%s; jobids=%s' %(repr(jobid), repr(jobids)))
            #if jobid not in jobid2mjob:
            #    log.info("jobid=%s is not known. Might have been deleted already." %jobid)
            yield jobid
    else:
        raise Exception('which=%s'%repr(which))
def delete_jobid(state, jobid, keep=False):
    """
    Kill the job with this heartbeat.
    (If there is no heartbeat, then the job is already gone.)
    Delete the entry from state and update its jobid.
    """
    try:
        bjob = state.get_bjob(jobid)
    except Exception:
        log.exception('In delete_jobid(), unable to find batchjob for %s' %(jobid))
        # TODO: Maybe provide a default grid type, so we can attempt to delete anyway?
        return
    try:
        bjob.kill(state, jobid)
    except Exception as exc:
        log.debug('Failed to kill job for jobid {!r}: {!r}'.format(
            jobid , exc))
    state.add_deleted_jobid(jobid)
    # For now, keep it in the 'jobs' table.
    hsocket = socket.socket()
    try:
        hsocket.connect(state.get_heartbeat_server())
        socket_send(hsocket, 'd {}'.format(jobid))
        hsocket.close()
        log.debug('Removed jobid=%s' %repr(jobid))
    except IOError as e:
        log.debug('Cannot remove jobid {}: {!r}'.format(jobid, e))
def cmd_delete(state, which, jobids):
    """Kill designated jobs, including (hopefully) their
    entire process groups.
    If which=='list', then kill all jobs listed as jobids.
    If which=='known', then kill all known jobs.
    If which=='infer', then kill all jobs with heartbeats.
    Remove those heartbeat files.
    """
    log.debug('Deleting jobs for jobids from %s (%s)' %(
        which, repr(jobids)))
    for jobid in find_jobids(state, which, jobids):
        delete_jobid(state, jobid)
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
        raise Exception('network_based pwatcher is currently broken.')
        self.state = state

def get_process_watcher(directory):
    state = get_state(directory)
    #log.debug('state =\n%s' %pprint.pformat(state.top))
    return ProcessWatcher(state)
    #State_save(state)

@contextlib.contextmanager
def process_watcher(directory, hostname='', port=0):
    """This will (someday) hold a lock, so that
    the State can be written safely at the end.
    """
    state = get_state(directory, hostname, port)
    #log.debug('state =\n%s' %pprint.pformat(state.top))
    yield ProcessWatcher(state)
    # TODO: Sometimes, maybe we should not save state.
    # Or maybe we *should* on exception.
    state.save()

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
            print(pprint.pformat(result))


# With bash, we would need to set the session, rather than
# the process group. That's not ideal, but this is here for reference.
#  http://stackoverflow.com/questions/6549663/how-to-set-process-group-of-a-shell-script
#
bash_template = """#!%(lang_exe)s
cmd='%(cmd)s'
"$cmd"
"""

# perl might be better, for efficiency.
# But we will use python for now.
#
python_template = r"""#!%(lang_exe)s
import threading, time, os, sys, subprocess, socket

cmd='%(cmd)s'
heartbeat_server=('%(heartbeat_server)s' %(heartbeat_port)s)
jobid='%(jobid)s'
sleep_s=%(sleep_s)s
cwd='%(cwd)s'

os.chdir(cwd)

def socket_send(socket, message):
    socket.sendall(b'{}\0'.format(message))

def log(msg):
    hsocket = socket.socket()
    try:
        hsocket.connect(heartbeat_server)
        socket_send(hsocket, 's {} {}\n'.format(jobid, msg))
        hsocket.close()
    except IOError:             # better to miss a line than terminate
        pass

def thread_heartbeat():
    pid = os.getpid()
    pgid = os.getpgid(0)
    hsocket = socket.socket()
    try:
        hsocket.connect(heartbeat_server)
        socket_send(hsocket, 'i {} {} {}'.format(jobid, pid, pgid))
        hsocket.close()
    except IOError:	# hope it's a temporary error
        pass
    while True:
        time.sleep(sleep_s)
        hsocket = socket.socket()
        try:
            hsocket.connect(heartbeat_server)
            socket_send(hsocket, 'h {}'.format(jobid))
            hsocket.close()
        except IOError:	# hope it's a temporary error
            pass

def start_heartbeat():
    hb = threading.Thread(target=thread_heartbeat)
    log('alive? {}'.format(hb.is_alive()))
    hb.daemon = True
    hb.start()
    return hb

def main():
    log('cwd:{!r}'.format(os.getcwd()))
    log("before: pid={}s pgid={}s".format(os.getpid(), os.getpgid(0)))
    try:
        os.setpgid(0, 0)
    except OSError as e:
        log('Unable to set pgid. Possibly a grid job? Hopefully there will be no dangling processes when killed: {}'.format(
            repr(e)))
    log("after: pid={}s pgid={}s".format(os.getpid(), os.getpgid(0)))
    hb = start_heartbeat()
    log('alive? {} pid={} pgid={}'.format(hb.is_alive(), os.getpid(), os.getpgid(0)))
    sp = subprocess.Popen(shlex.split(cmd), stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    with sp.stdout as f:
        for line in iter(f.readline, b''):
            log(heartbeat_server, jobid, line)
    rc = sp.wait()
    # sys.exit(rc) # No-one would see this anyway.
    hsocket = socket.socket()
    try:
        hsocket.connect(heartbeat_server)
        socket_send(hsocket, 'e {} {}'.format(jobid, rc))
        hsocket.close()
    except IOError as e:
        log('could not update heartbeat server with exit status: {} {}: {!r}'.format(jobid, rc, e))
    if rc:
        raise Exception('{} <- {!r}'.format(rc, cmd))
main()
"""

if __name__ == "__main__":
    import pdb
    pdb.set_trace()
    main(*sys.argv) # pylint: disable=no-value-for-parameter
