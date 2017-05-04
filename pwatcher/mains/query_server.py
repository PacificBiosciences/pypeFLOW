#!/usr/bin/env python

"""
Query the heartbeat server from the command line.
As an argument, takes either server:port or the falcon run directory
(if not argument is given, uses the current directory).
"""

import argparse
import collections
import os
import re
import socket
import sys

STATE_FN = 'state.py'		# taken from network_based.py
STATE_DIR = 'mypwatcher'	# taken from pwatcher_bridge.py

# send message delimited with a \0
def socket_send(socket, message):
    socket.sendall(b'{}\0'.format(message))

# receive all of \0 delimited message
# may discard content past \0, if any, so not safe to call twice on same socket
def socket_read(socket):
    buffer = bytearray(b' ' * 256)
    nbytes = socket.recv_into(buffer, 256)
    if nbytes == 0:             # empty message
        return
    message = ''
    while nbytes != 0:
        try:    # index() raises when it can't find the character
            i = buffer[:nbytes].index('\0')
            message += str(buffer[:i])  # discard past delimiter
            break
        except ValueError:      # haven't reached end yet
            message += str(buffer)
        nbytes = socket.recv_into(buffer, 256)
    return message

# get server and port from watcher state file
def use_state(filename):
    with open(filename, 'r') as f:
        for line in f:
            match = re.match(r" 'server': \('([^']+)', (\d+)\)", line)
            if match:
                return (match.group(1), int(match.group(2)))
    print('Error: could not find server info in state file {}'.format(filename))

def parse_args():
    parser = argparse.ArgumentParser(description='query falcon network based heartbeat server')
    parser.add_argument('-s', '--server', help='<server_name:port>')
    parser.add_argument('-f', '--file', help='location of pwatcher state file')
    parser.add_argument('-d', '--debug', default=False, action='store_const', const=True, help='get server state instead of process list')
    parser.add_argument('sf', nargs='?', help='specify server or file')
    return parser.parse_args()

# parse command line argument (if any) to find server info
def find_server(args):
    i = 0
    if args.server:
        i += 1
    if args.file:
        i += 1
    if args.sf:
        i += 1
    if i > 1:
        raise Exception('Error: may only specify server once. Try "--help".')
        return
    if args.sf:
        if os.path.exists(args.sf):
            args.file = args.sf
        else:
            try:
                args.sf.index(':')
            except ValueError:
                print('Error: could not parse argument as file or server:port: {}'.format(args.sf))
                return
            args.server = args.sf
    if args.server:
        try:
            i = args.server.index(':')
        except ValueError:
            print('Error: could not parse argument as server:port: {}'.format(args.server))
            return
        server = args.server[:i]
        port = int(args.server[i + 1:])
        return (server, port)
    if not args.file:
        args.file = '.'
    if os.path.isfile(args.file):
        return use_state(args.file)
    elif os.path.isdir(args.file):
        if os.path.isfile(os.path.join(args.file, STATE_FN)):
            return use_state(os.path.join(args.file, STATE_FN))
        elif os.path.isfile(os.path.join(args.file, STATE_DIR, STATE_FN)):
            return use_state(os.path.join(args.file, STATE_DIR, STATE_FN))
    print('Error: could not find state file: {}'.format(args.file))

args = parse_args()
server = find_server(args)
if not server:
    sys.exit(1)
s = socket.socket()
s.connect(server)

if args.debug:
    socket_send(s, 'D')
    server_state = socket_read(s)
    s.close()
    state = eval(server_state)
    for jobid, val in state.iteritems():
        print('{}: {} {} {} {}'.format(jobid, val[0], val[1], val[2], val[3]))
else:
    socket_send(s, 'L')
    jobids = socket_read(s)
    s.close()
    for jobid in jobids.split():
        s = socket.socket()
        s.connect(server)
        socket_send(s, 'Q {}'.format(jobid))
        m = socket_read(s)
        s.close()
        print('{} {}'.format(jobid, m))
