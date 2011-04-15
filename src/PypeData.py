
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

PypeData: This module defines the general interface and class for PypeData Objects. 

"""


from urlparse import urlparse
import platform
import os
from PypeCommon import * 
import logging
    
class FileNotExistError(PypeError):
    pass

def fn(obj):
    return obj.localFileName

class PypeDataObjectBase(PypeObject):
    
    """ 
    Represent the common interface for a PypeData object.
    """

    def __init__(self, URL, **attributes):
        PypeObject.__init__(self, URL, **attributes)
        self._log = logging.Logger('dataobject')

    @property
    def timeStamp(self):
        raise NotImplementedError

    @property
    def exists(self):
        raise NotImplementedError

class PypeLocalFile(PypeDataObjectBase):

    """ 
    Represent a PypeData object that can be accessed as a file in a local
    filesystem.

    >>> f = PypeLocalFile("file://localhost/test.txt")
    >>> f.localFileName == "test.txt"
    True
    >>> fn(f)
    'test.txt'
    >>> f = PypeLocalFile("file://localhost/test.txt", False, isFasta = True)
    >>> f.isFasta == True
    True
    """

    supportedURLScheme = ["file"]
    def __init__(self, URL, readOnly = True, **attributes):
        PypeDataObjectBase.__init__(self, URL, **attributes)
        URLParseResult = urlparse(URL)
        self.localFileName = URLParseResult.path[1:]
        self._path = self.localFileName
        self.readOnly = readOnly

    @property
    def timeStamp(self):
        if not os.path.exists(self.localFileName):
            raise FileNotExistError("No such file:%s on %s" % (self.localFileName, platform.node()) )
        return os.stat(self.localFileName).st_mtime 

    @property
    def exists(self):
        return os.path.exists(self.localFileName)
    
    def setVerifyFunction( self, verifyFunction ):
        self._verify = verifyFunction
    
    def verify( self ):
        if self._verify == None:
            return True
        self._log.debug("Verifying contents of %s" % self.URL)
        errors = self._verify( self.path )
        if len(errors) > 0:
            for e in errors:
                self._log.error(e)
        return len(errors) == 0
    
    def __str__( self ):
        return self.URL
    
    @property
    def path( self ):
        if self._path == None:
            raise IOError, "Must resolve this file (%s) with a context " + \
                            "before you can access .path"
        return self._path
    
    def clean( self ):
        if os.path.exists( self.path ):
            self._log.info("Removing %s" % self.path )
            if os.path.isdir( self.path ):
                shutil.rmtree( self.path )
            else:
                os.remove( self.path )

class PypeHDF5Dataset(PypeDataObjectBase):  #stub for now Mar 17, 2010

    """ 
    Represent a PypeData object that is an HDF5 dataset.
    Not implemented yet.
    """

    supportedURLScheme = ["hdf5ds"]
    def __init__(self, URL, readOnly = True, **attributes):
        PypeDataObjectBase.__init__(self, URL, **attributes)
        URLParseResult = urlparse(URL)
        self.localFileName = URLParseResult.path[1:]
        #the rest of the URL goes to HDF5 DS

class PypeLocalCompositeFile(PypeDataObjectBase):  #stub for now Mar 17, 2010

    """ 
    Represent a PypeData object that is a composition of multiple files.
    Not implemented yet.
    """

    supportedURLScheme = ["compositeFile"]
    def __init__(self, URL, readOnly = True, **attributes):
        PypeDataObjectBase.__init__(self, URL, **attributes)
        URLParseResult = urlparse(URL)
        self.localFileName = URLParseResult.path[1:]

def makePypeLocalFile(aLocalFileName, readOnly = True, **attributes):
    """
    >>> f = makePypeLocalFile("./test.txt")
    >>> f.localFileName == "./test.txt"
    True
    >>> fn(f)
    './test.txt'
    """
    return PypeLocalFile("file://localhost/%s" % aLocalFileName, readOnly, **attributes)

if __name__ == "__main__":
    import doctest
    doctest.testmod()
    
