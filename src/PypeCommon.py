from urlparse import urlparse
from rdflib.Graph import ConjunctiveGraph as Graph
from rdflib import Namespace
from rdflib import Literal
from rdflib import URIRef
from subprocess import Popen
import time

pypeNS = Namespace("http://pype/v0.1/")

class URLSchemeNotSupportYet(Exception):
    def __init__(self, msg):
        self.msg = msg
    def __str__(self):
        return repr(slef.msg)

class PypeObject(object):
    """ 
    Base class for all PypeObjects
    Every PypeObject should have an URL, and when _updataRDFGraph got called, it will generate the RDF XML for the object. 
    The instance attributes can be set by using key-work argument. This design is for convinenice for now. Since it is not quite self,
    we might remove it in the furture. 
    """
    def __init__(self, URL, **attributes):
        self._RDFGraph = None
        URLParseResult = urlparse(URL)
        if URLParseResult.scheme not in self.__class__.supportedURLScheme:
            raise URLSchemeNotSupportYet("%s is not supported yet")
        else:
            self.URL = URL
            for k,v in attributes.iteritems():
                if k not in self.__dict__:
                    self.__dict__[k] = v
        self._updateRDFGraph()
        
    def _updateRDFGraph(self):
        graph = self._RDFGraph = Graph()
        for k,v in self.__dict__.iteritems():
            if k == "URL": continue
            if k[0] == "_": continue
            if hasattr(v, "URL"):
                graph.add( ( URIRef(self.URL), pypeNS[k], URIRef(v.URL) ) )
            else:
                graph.add( ( URIRef(self.URL), pypeNS[k], Literal(json.dumps(v)) ) )
    
    @property
    def RDFXML(self):
        return self._RDFGraph.serialize() 


def runShellCmd(args):
    p = Popen(args)
    pStatus = None
    while 1:
        time.sleep(0.1)
        pStatus = p.poll()
        if pStatus != None:
            break
    return pStatus

def runClusterCmd(args):
    p = Popen(args)
    pStatus = None
    while 1:
        time.sleep(0.1)
        pStatus = p.poll()
        if pStatus != None:
            break
    return pStatus
