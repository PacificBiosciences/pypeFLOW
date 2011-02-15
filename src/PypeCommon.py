from rdflib import Namespace

pypeNS = Namespace("http://pype/v0.1/")

class URLSchemeNotSupportYet(Exception):
    def __init__(self, msg):
        self.msg = msg
    def __str__(self):
        return repr(slef.msg)
