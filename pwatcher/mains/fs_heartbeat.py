"""Filesystem heartbeat wrapper

Perl might be better for efficiency.
But we will use python for now.

Non-zero status means *this* failed, not the wrapped command.
"""
import argparse
import os
import socket
import sys
import threading
import time

DESCRIPTION = """
We wrap a system call to produce both a heartbeat and an exit-sentinel
in the filesystem.
"""
EPILOG = """
We share stderr/stdout with the command. We log to stderr (for now).
"""
HEARTBEAT_TEMPLATE = '0 {pid} {pgid}\n'
EXIT_TEMPLATE = '{exit_code}'

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
        type=float,
        default=1.0, # TODO: Make this at least 10, maybe 60.
    )
    parser.add_argument('--heartbeat-file',
        help='Path to heartbeat file. The first line will have the format {!r}. The rest are just elapsed time'.format(
            HEARTBEAT_TEMPLATE),
        required=True,
    )
    parser.add_argument('--exit-file',
        help='Path to exit sentinel file. At end, it will have the format {!r}'.format(
            EXIT_TEMPLATE),
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

def log(msg):
    sys.stderr.write(msg)
    sys.stderr.write('\n')
    #sys.stdout.flush() # If we use stdout.

def thread_heartbeat(heartbeat_fn, sleep_s):
    with open(heartbeat_fn, 'w') as ofs:
        pid = os.getpid()
        pgid = os.getpgid(0)
        ofs.write(HEARTBEAT_TEMPLATE.format(
            **locals()))
        elapsed = 0
        ctime = 0
        while True:
            #ctime = time.time()
            ofs.write('{elapsed} {ctime}\n'.format(
                **locals()))
            ofs.flush()
            time.sleep(sleep_s)
            elapsed += 1

def start_heartbeat(heartbeat_fn, sleep_s):
    hb = threading.Thread(target=thread_heartbeat, args=(heartbeat_fn, sleep_s))
    log('alive? {}'.format(
        bool(hb.is_alive())))
    hb.daemon = True
    hb.start()
    return hb

def run(args):
    os.chdir(args.directory)
    heartbeat_fn = os.path.abspath(args.heartbeat_file)
    exit_fn = os.path.abspath(args.exit_file)
    cwd = os.getcwd()
    hostname = socket.getfqdn()
    sleep_s = args.rate
    log("""
cwd:{cwd!r}
hostname={hostname}
heartbeat_fn={heartbeat_fn!r}
exit_fn={exit_fn!r}
sleep_s={sleep_s!r}""".format(
        **locals()))
    if os.path.exists(exit_fn):
        os.remove(exit_fn)
    if os.path.exists(heartbeat_fn):
        os.remove(heartbeat_fn)
    #os.system('touch {}'.format(heartbeat_fn)) # This would be over-written anyway.
    log("before setpgid: pid={} pgid={}".format(os.getpid(), os.getpgid(0)))
    try:
        os.setpgid(0, 0) # This allows the entire tree of procs to be killed.
        log(" after setpgid: pid={} pgid={}".format(
            os.getpid(), os.getpgid(0)))
    except OSError as e:
        log(' Unable to set pgid. Possibly a grid job? Hopefully there will be no dangling processes when killed: {}'.format(
            repr(e)))

    #thread = start_heartbeat(heartbeat_fn, sleep_s)

    #log('alive? {} pid={} pgid={}'.format(
    #    bool(thread.is_alive()), os.getpid(), os.getpgid(0)))

    call = ' '.join(args.command)
    log('In cwd: {}, Blocking call: {!r}'.format(
        os.getcwd(), call))
    rc = os.system(call) # Blocking.

    log(' returned: {!r}'.format(
        rc))

    # Do not delete the heartbeat here. The discoverer of the exit-sentinel will do that,
    # to avoid a race condition.
    #if os.path.exists(heartbeat_fn):
    #    os.remove(heartbeat_fn)

    exit_tmp_fn = exit_fn + '.tmp'
    with open(exit_tmp_fn, 'w') as ofs:
        ofs.write(EXIT_TEMPLATE.format(
            exit_code=rc))
    os.rename(exit_tmp_fn, exit_fn) # atomic
    # sys.exit(rc) # No-one would see this anyway.

def main():
    args = parse_args(sys.argv[1:])
    log(repr(args))
    run(args)

if __name__ == "__main__":
    main()
