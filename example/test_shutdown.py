
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

import sys
import os 


from pypeflow.common import * 
from pypeflow.task import PypeThreadTaskBase, PypeTaskBase
from pypeflow.task import PypeTask, PypeShellTask, PypeSGETask, PypeDistributibleTask
from pypeflow.controller import PypeWorkflow, PypeThreadWorkflow, PypeMPWorkflow
from pypeflow.data import PypeLocalFile, makePypeLocalFile, fn
import logging
import time

logger = logging.getLogger()
#logger.setLevel(logging.INFO)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
ch.setFormatter(formatter)
logger.addHandler(ch)

inputs = {"input": makePypeLocalFile("/tmp/test1_input")}
outputs = {"output": makePypeLocalFile("/tmp/test1_output")}
os.system("touch /tmp/test1_input")

@PypeTask(inputs = inputs, outputs = outputs, TaskType = PypeThreadTaskBase)
def f(self):
    i = 0
    while 1:
        time.sleep(0.1)
        if self.shutdown_event != None and self.shutdown_event.is_set():
            break
        if i > 10:
            break
        i += 1
    if self.shutdown_event == None or not self.shutdown_event.is_set():
        os.system("touch %s" % fn(self.output))

wf = PypeThreadWorkflow()
wf.addTasks([f])
wf.refreshTargets()







