"""Network server heartbeat wrapper

Perl might be better for efficiency.
But we will use python for now.

Non-zero status means *this* failed, not the wrapped command.
"""
import argparse
import os
import shlex
import socket
import subprocess
import sys
import threading
import time

DESCRIPTION = """
We wrap a system call to produce a heartbeat.
"""
EPILOG = """
We log to the status server, and forward command stdout/stderr as well.
"""

class _Formatter(argparse.RawDescriptionHelpFormatter, argparse.ArgumentDefaultsHelpFormatter):
    pass
_FORMATTER_CLASS = _Formatter

def parse_args(args):
    parser = argparse.ArgumentParser(
        description=DESCRIPTION,
        epilog=EPILOG,
        formatter_class=_FORMATTER_CLASS,
    )
    parser.add_argument('--rate',
        help='Heartbeat rate, in seconds',
        type=int,
        default=600,
    )
    parser.add_argument('--heartbeat-server',
        help='Address of the heartbeat server',
        required=True,
    )
    parser.add_argument('--heartbeat-port',
        help='Port of the heartbeat server',
        type=int,
        required=True,
    )
    parser.add_argument('--jobid',
        help='Our jobid',
        required=True,
    )
    parser.add_argument('--exit-dir',
        help='Path to emergency exit sentinel directory',
        required=True,
    )
    parser.add_argument('--directory',
        help='Directory in which to run COMMAND.',
        default='.',
    )
    parser.add_argument('command',
        help='System call (to be joined by " "). We will block on this and return its result.',
        nargs='+',
        #required=True,
    )
    return parser.parse_args(args)

# send message delimited with a \0
def socket_send(socket, message):
    socket.sendall(b'{}\0'.format(message))

def log(heartbeat_server, jobid, msg):
    hsocket = socket.socket()
    try:
        hsocket.connect(heartbeat_server)
        socket_send(hsocket, 's {} {}\n'.format(jobid, msg))
        hsocket.close()
    except IOError:		# better to miss a line than terminate
        pass

def thread_heartbeat(heartbeat_server, jobid, sleep_s):
    pid = os.getpid()
    pgid = os.getpgid(0)
    hsocket = socket.socket()
    try:
        hsocket.connect(heartbeat_server)
        socket_send(hsocket, 'i {} {} {}'.format(jobid, pid, pgid))
        hsocket.close()
    except IOError:	# we hope it's a temporary error
        pass
    while True:
        time.sleep(sleep_s)
        hsocket = socket.socket()
        try:
            hsocket.connect(heartbeat_server)
            socket_send(hsocket, 'h {}'.format(jobid))
            hsocket.close()
        except IOError:	# we hope it's a temporary error
            pass

def start_heartbeat(heartbeat_server, jobid, sleep_s):
    hb = threading.Thread(target=thread_heartbeat, args=(heartbeat_server, jobid, sleep_s))
    log(heartbeat_server, jobid, 'alive? {}'.format(
        bool(hb.is_alive())))
    hb.daemon = True
    hb.start()
    return hb

def run(args):
    heartbeat_server = (args.heartbeat_server, args.heartbeat_port)
    jobid = args.jobid
    log(heartbeat_server, jobid, repr(args))
    os.chdir(args.directory)
    exit_dir = args.exit_dir
    exit_fn = os.path.join(os.path.abspath(exit_dir), jobid)
    cwd = os.getcwd()
    hostname = socket.getfqdn()
    sleep_s = args.rate
    log(heartbeat_server, jobid, """
cwd:{cwd!r}
hostname={hostname}
heartbeat_server={heartbeat_server!r}
jobid={jobid}
exit_dir={exit_dir!r}
sleep_s={sleep_s!r}""".format(
        **locals()))
    log(heartbeat_server, jobid, "before setpgid: pid={} pgid={}".format(os.getpid(), os.getpgid(0)))
    try:
        os.setpgid(0, 0) # This allows the entire tree of procs to be killed.
        log(heartbeat_server, jobid, " after setpgid: pid={} pgid={}".format(
            os.getpid(), os.getpgid(0)))
    except OSError as e:
        log(heartbeat_server, jobid, ' Unable to set pgid. Possibly a grid job? Hopefully there will be no dangling processes when killed: {}'.format(
            repr(e)))

    thread = start_heartbeat(heartbeat_server, jobid, sleep_s)

    log(heartbeat_server, jobid, 'alive? {} pid={} pgid={}'.format(
        bool(thread.is_alive()), os.getpid(), os.getpgid(0)))

    call = ' '.join(args.command)
    log(heartbeat_server, jobid, 'In cwd: {}, Blocking call: {!r}'.format(
        os.getcwd(), call))
    sp = subprocess.Popen(shlex.split(call), stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    # forward all output to server until job ends, then get exit value
    with sp.stdout as f:
        for line in iter(f.readline, b''):
            # can't use log() for this because it appends a \n
            hsocket = socket.socket()
            try:
                hsocket.connect(heartbeat_server)
                socket_send(hsocket, 's {} {}'.format(jobid, line))
                hsocket.close()
            except IOError:		# better to miss a line than terminate
                pass
    rc = sp.wait()

    log(heartbeat_server, jobid, ' returned: {!r}'.format(
        rc))

    hsocket = socket.socket()
    try:
        hsocket.connect(heartbeat_server)
        socket_send(hsocket, 'e {} {}'.format(jobid, rc))
        hsocket.close()
    except IOError as e:
        log(heartbeat_server, jobid, 'could not update heartbeat server with exit status: {} {}: {!r}'.format(jobid, rc, e))
        with open(exit_fn, 'w') as f:
            f.write(str(rc))
    # sys.exit(rc) # No-one would see this anyway.

def main():
    args = parse_args(sys.argv[1:])
    run(args)

if __name__ == "__main__":
    main()
