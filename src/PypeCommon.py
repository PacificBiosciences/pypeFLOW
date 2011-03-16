from rdflib import Namespace
from subprocess import Popen
import time

pypeNS = Namespace("http://pype/v0.1/")

class URLSchemeNotSupportYet(Exception):
    def __init__(self, msg):
        self.msg = msg
    def __str__(self):
        return repr(slef.msg)

def runShellCmd(args):
    p = Popen(args)
    pStatus = None
    while 1:
        time.sleep(0.1)
        pStatus = p.poll()
        if pStatus != None:
            break
    return pStatus
