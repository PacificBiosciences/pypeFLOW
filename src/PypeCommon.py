


from urlparse import urlparse

import rdflib
try:
    from rdflib.Graph import ConjunctiveGraph as Graph #work for rdflib-2.4.2
except:
    from rdflib import ConjunctiveGraph as Graph #work for rdflib-3.0.0, need to patch rdflib.graph.query to support initNs
    """
    in order to work with rdflib-3.0.0, patch to rdflib-3.0.0 source code needed 
    in rdflib.graph: one needs to add the initNs and initBindings to the following two line
    1) def query(..., initNs={}, initBindings={})
    2) return result(processor.query(query_object, initBindings, initNs))
    """
    # need to install rdfextras for rdflib-3.0.0
    rdflib.plugin.register('sparql', rdflib.query.Processor,
                           'rdfextras.sparql.processor', 'Processor')
    rdflib.plugin.register('sparql', rdflib.query.Result,
                           'rdfextras.sparql.query', 'SPARQLQueryResult')
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
        return repr(self.msg)

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
            raise URLSchemeNotSupportYet("%s is not supported yet" % URLParseResult.scheme )
        else:
            self.URL = URL
            for k,v in attributes.iteritems():
                if k not in self.__dict__:
                    self.__dict__[k] = v
        # this is typically called in __init__ by child classes -
        # breaks if you call it here for Tasks and have no inputDataObj
        # self._updateRDFGraph() 
        
    def _updateRDFGraph(self):
        graph = self._RDFGraph = Graph()
        for k,v in self.__dict__.iteritems():
            if k == "URL": continue
            if k[0] == "_": continue
            if hasattr(v, "URL"):
                graph.add( ( URIRef(self.URL), pypeNS[k], URIRef(v.URL) ) )
    
    @property
    def RDFXML(self):
        return self._RDFGraph.serialize() 


def runShellCmd(args):
    p = Popen(args)
    pStatus = None
    while 1:
        time.sleep(0.2)
        pStatus = p.poll()
        if pStatus != None:
            break
    return pStatus

def runSgeSyncJob(args):
    p = Popen(args)
    pStatus = None
    while 1:
        time.sleep(0.1)
        pStatus = p.poll()
        if pStatus != None:
            break
    return pStatus
