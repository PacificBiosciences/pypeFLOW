
# @author Jason Chin
#
# Copyright (C) 2010 by Jason Chin 
# Copyright (C) 2011 by Jason Chin
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.

"""

PypeCommon: provide the common base classes and general module level constants for PypeEngine

"""

from urlparse import urlparse

import rdflib
try:
    from rdflib.Graph import ConjunctiveGraph as Graph #work for rdflib-2.4.2
except:
    from rdflib import ConjunctiveGraph as Graph #work for rdflib-3.1.0
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

pypeNS = Namespace("pype://v0.1/")

class NotImplementedError(Exception):
    def __init__(self, msg):
        self.msg = msg
    def __str__(self):
        return repr(self.msg)

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
