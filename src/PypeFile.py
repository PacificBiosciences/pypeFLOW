from urlparse import urlparse

import json
from rdflib.Graph import ConjunctiveGraph as Graph
from rdflib import Namespace
from rdflib import Literal
from rdflib import URIRef
from PypeCommon import pypeNS, URLSchemeNotSupportYet
    
def fn(obj):
    return obj.localFileName

class PypeFile(object):
    supportedURLScheme = ["file"]
    _URL2Instance = {}
    def __new__(cls, fileURL, *args, **kwargs):
        if fileURL in cls._URL2Instance:
            return URL2Instance[fileURL]
        else:
            cls._instance = super(PypeFile, cls).__new__(cls, fileURL, *args, **kwargs)
            cls._URL2Instance[fileURL] = cls._instance
        return cls._instance
    def __init__(self, fileURL, readOnly = True, **attributes):
        self._RDFGraph = None
        URLParseResult = urlparse(fileURL)
        if URLParseResult.scheme not in PypeFile.supportedURLScheme:
            raise URLSchemeNotSupportYet("%s is not supported yet")
        else:
            self.localFileName = URLParseResult.path[1:]
            self.URL = fileURL
            self.readOnly = readOnly
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

def makeLocalPypeFile(aLocalFileName, readOnly = True, **attributes):
    return PypeFile("file://localhost/./%s" % aLocalFileName, readOnly, **attributes)

def test():
    f = PypeFile("file://localhost/./test.txt")
    assert f.localFileName == "./test.txt"
    
    f = PypeFile("file://localhost/./test.txt", False)
    assert f.readOnly == False

    f = PypeFile("file://localhost/./test.txt", False, isFasta = True)
    assert f.isFasta == True

    f.generateBy = "test"
    print f.RDFXML
        
if __name__ == "__main__":
    test()
        
