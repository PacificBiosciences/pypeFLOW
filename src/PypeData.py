from urlparse import urlparse

import json
from rdflib.Graph import ConjunctiveGraph as Graph
from rdflib import Namespace
from rdflib import Literal
from rdflib import URIRef
from PypeCommon import PypeObject, pypeNS, URLSchemeNotSupportYet
    
def fn(obj):
    return obj.localFileName

class PypeDataObjectBase(PypeObject):
    def __init__(self, URL, **attributes):
        PypeObject.__init__(self, URL, **attributes)


class PypeLocalFile(PypeDataObjectBase):
    supportedURLScheme = ["file"]
    def __init__(self, URL, readOnly = True, **attributes):
        PypeDataObjectBase.__init__(self, URL, **attributes)
        URLParseResult = urlparse(URL)
        self.localFileName = URLParseResult.path[1:]

def makePypeLocalFile(aLocalFileName, readOnly = True, **attributes):
    return PypeLocalFile("file://localhost/./%s" % aLocalFileName, readOnly, **attributes)

def test():
    f = PypeLocalFile.gc("file://localhost/./test.txt")
    assert f.localFileName == "./test.txt"
    
    f = PypeLocalFile.gc("file://localhost/./test.txt", False)
    assert f.readOnly == False

    f = PypeLocalFile.gc("file://localhost/./test.txt", False, isFasta = True)
    assert f.isFasta == True

    f.generateBy = "test"
    print f.RDFXML
        
if __name__ == "__main__":
    test()
        
