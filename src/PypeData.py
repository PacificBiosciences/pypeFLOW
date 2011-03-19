from urlparse import urlparse

import json
from PypeCommon import * 
    
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

class PypeHDF5Dataset(PypeDataObjectBase):  #stub for now Mar 17, 2010
    supportedURLScheme = ["hdf5ds"]
    def __init__(self, URL, readOnly = True, **attributes):
        PypeDataObjectBase.__init__(self, URL, **attributes)
        URLParseResult = urlparse(URL)
        self.localFileName = URLParseResult.path[1:]
        #the rest of the URL goes to HDF5 DS

class PypeLocalCompositeFile(PypeDataObjectBase):  #stub for now Mar 17, 2010
    supportedURLScheme = ["compositeFile"]
    def __init__(self, URL, readOnly = True, **attributes):
        PypeDataObjectBase.__init__(self, URL, **attributes)
        URLParseResult = urlparse(URL)
        self.localFileName = URLParseResult.path[1:]
        #the rest of the URL goes to HDF5 DS

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
        
