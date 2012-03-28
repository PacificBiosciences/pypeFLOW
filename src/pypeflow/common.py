
# @author Jason Chin
#
# Copyright (C) 2010 by Jason Chin 
# Copyright (C) 2011 by Jason Chin, Pacific Biosciences
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

PypeCommon: provide the common base classes and general module level utility functions
            and constants for PypeEngine

"""

from urlparse import urlparse

import rdflib
try:
    from rdflib import ConjunctiveGraph as Graph #work for rdflib-3.1.0
    # need to install rdfextras for rdflib-3.0.0
    rdflib.plugin.register('sparql', rdflib.query.Processor,
                           'rdfextras.sparql.processor', 'Processor')
    rdflib.plugin.register('sparql', rdflib.query.Result,
                           'rdfextras.sparql.query', 'SPARQLQueryResult')
except:
    from rdflib.Graph import ConjunctiveGraph as Graph #work for rdflib-2.4.2
from rdflib import Namespace
from rdflib import Literal
from rdflib import URIRef

from subprocess import Popen, PIPE
import time

pypeNS = Namespace("pype://v0.1/")

class PypeError(Exception):
    def __init__(self, msg):
        self.msg = msg
    def __str__(self):
        return repr(self.msg)

class NotImplementedError(PypeError):
    pass

class URLSchemeNotSupportYet(PypeError):
    pass

class PypeObject(object):

    """ 

    Base class for all PypeObjects

    Every PypeObject should have an URL.
    The instance attributes can be set by using keyword argument in __init__(). 

    """

    def __init__(self, URL, **attributes):

        URLParseResult = urlparse(URL)
        if URLParseResult.scheme not in self.__class__.supportedURLScheme:
            raise URLSchemeNotSupportYet("%s is not supported yet" % URLParseResult.scheme )
        else:
            self.URL = URL
            for k,v in attributes.iteritems():
                if k not in self.__dict__:
                    self.__dict__[k] = v

    def _updateURL(self, newURL):
        URLParseResult = urlparse(self.URL)
        newURLParseResult = urlparse(newURL)
        if URLParseResult.scheme != newURLParseResult.scheme:
            raise PypeError, "the URL scheme can not be changed for obj %s" % self.URL
        self.URL = newURL
     
    @property 
    def _RDFGraph(self):
        graph = Graph()

        for k, v in self.__dict__.iteritems():
            if k == "URL": continue
            if k[0] == "_": continue
            if hasattr(v, "URL"):
                graph.add( ( URIRef(self.URL), pypeNS[k], URIRef(v.URL) ) )
        return graph


    
    @property
    def RDFXML(self):

        """ 
        RDF XML representation of the everything related to the PypeObject 
        """

        return self._RDFGraph.serialize() 

def runShellCmd(args,**kwargs):

    """ 
    Utility function that runs a shell script command. 
    It blocks until the command is finished. The return value
    from the shell command is returned
    """

    p = Popen(args,**kwargs)
    pStatus = None
    while 1:
        time.sleep(0.2)
        pStatus = p.poll()
        if pStatus != None:
            break
    return pStatus

def runSgeSyncJob(args):

    """ 
    Utility function that runs a shell script with SGE. 
    It blocks until the command is finished. The return value
    from the shell command is returned
    """

    p = Popen(args)
    pStatus = None
    while 1:
        time.sleep(0.1)
        pStatus = p.poll()
        if pStatus != None:
            break
    return pStatus
