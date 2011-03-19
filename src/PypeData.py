from urlparse import urlparse

import os
from PypeCommon import * 
    
class NotImplementedError(Exception):
    def __init__(self, msg):
        self.msg = msg
    def __str__(self):
        return repr(self.msg)


class FileNotExistError(Exception):
    def __init__(self, msg):
        self.msg = msg
    def __str__(self):
        return repr(self.msg)

def fn(obj):
    return obj.localFileName

class PypeDataObjectBase(PypeObject):
    def __init__(self, URL, **attributes):
        PypeObject.__init__(self, URL, **attributes)

    @property
    def timeStamp(self):
        raise NotImplementedError

    @property
    def isExist(self):
        raise NotImplementedError

class PypeLocalFile(PypeDataObjectBase):
    supportedURLScheme = ["file"]
    def __init__(self, URL, readOnly = True, **attributes):
        PypeDataObjectBase.__init__(self, URL, **attributes)
        URLParseResult = urlparse(URL)
        self.localFileName = URLParseResult.path[1:]

    @property
    def timeStamp(self):
        if not os.path.exists(self.localFileName):
            raise FileNotExistError("No such file:"+self.localFileName)
        return os.stat(self.localFileName).st_mtime 

    @property
    def isExist(self):
        return os.path.exists(self.localFileName)

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
    f = PypeLocalFile("file://localhost/./test.txt")
    assert f.localFileName == "./test.txt"
    
    f = PypeLocalFile("file://localhost/./test.txt", False)
    assert f.readOnly == False

    f = PypeLocalFile("file://localhost/./test.txt", False, isFasta = True)
    assert f.isFasta == True

    f.generateBy = "test"
    print f.RDFXML
        
if __name__ == "__main__":
    test()
        
