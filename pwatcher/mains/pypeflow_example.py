from pypeflow.simple_pwatcher_bridge import (PypeProcWatcherWorkflow, MyFakePypeThreadTaskBase,
        makePypeLocalFile, fn, PypeTask)
import json
import logging.config
import os
import sys

JOB_TYPE = os.environ.get('JOB_TYPE', 'local')
SLEEP_S = os.environ.get('SLEEP_S', '1')
log = logging.getLogger(__name__)

def spawn(args, check=False):
    cmd = args[0]
    log.debug('$(%s %s)' %(cmd, repr(args)))
    rc = os.spawnv(os.P_WAIT, cmd, args) # spawnvp for PATH lookup
    msg = "Call %r returned %d." % (cmd, rc)
    if rc:
        log.warning(msg)
        if check:
            raise Exception(msg)
    else:
        log.debug(msg)
    return rc
def system(call, check=False):
    log.debug('$(%s)' %repr(call))
    rc = os.system(call)
    msg = "Call %r returned %d." % (call, rc)
    if rc:
        log.warning(msg)
        if check:
            raise Exception(msg)
    else:
        log.debug(msg)
    return rc
def makedirs(d):
    if not os.path.isdir(d):
        os.makedirs(d)
def taskrun0(self):
    template = """
sleep_s=%(sleep_s)s
ofile=%(ofile)s

set -vex
echo start0
sleep ${sleep_s}
touch ${ofile}
echo end0
"""
    bash = template %dict(
        #ifile=fn(self.i0),
        ofile=fn(self.f0),
        sleep_s=self.parameters['sleep_s'],
    )
    log.debug('taskrun0 bash:\n' + bash)
    script = 'taskrun0.sh'
    with open(script, 'w') as ofs:
        ofs.write(bash)
    #system("bash {}".format(script), check=True)
    #spawn(['/bin/bash', script], check=True) # Beware! Hard to kill procs.
    self.generated_script_fn = script
    return script
def taskrun1(self):
    template = """
sleep_s=%(sleep_s)s
ifile=%(ifile)s
ofile=%(ofile)s

set -vex
echo start1
sleep ${sleep_s}
cp -f ${ifile} ${ofile}
echo end1
"""
    bash = template %dict(
        ifile=fn(self.f0),
        ofile=fn(self.f1),
        sleep_s=self.parameters['sleep_s'],
    )
    log.debug('taskrun1 bash:\n' + bash)
    script = 'taskrun1.sh'
    with open(script, 'w') as ofs:
        ofs.write(bash)
    #system("bash {}".format(script), check=True)
    self.generated_script_fn = script
    return script

def main():
    lfn = 'logging-cfg.json'
    if os.path.exists(lfn):
        logging.config.dictConfig(json.load(open(lfn)))
    else:
        logging.basicConfig()
        logging.getLogger().setLevel(logging.NOTSET)
        try:
            import logging_tree
            logging_tree.printout()
        except ImportError:
            pass
    log.debug('DEBUG LOGGING ON')
    log.warning('Available via env: JOB_TYPE={}, SLEEP_S={}'.format(
        JOB_TYPE, SLEEP_S))
    exitOnFailure=False
    concurrent_jobs=2
    Workflow = PypeProcWatcherWorkflow
    wf = Workflow(job_type=JOB_TYPE)
    wf.max_jobs = concurrent_jobs

    par = dict(sleep_s=SLEEP_S)
    DIR ='mytmp'
    makedirs(DIR)
    f0 = makePypeLocalFile('mytmp/f0')
    f1 = makePypeLocalFile('mytmp/f1')
    make_task = PypeTask(
            inputs = {},
            outputs = {'f0': f0},
            parameters = par,
    )
    task = make_task(taskrun0)
    wf.addTasks([task])
    make_task = PypeTask(
            inputs = {'f0': f0},
            outputs = {'f1': f1},
            parameters = par,
    )
    task = make_task(taskrun1)
    wf.addTasks([task])
    wf.refreshTargets([task])
    #wf.refreshTargets(exitOnFailure=exitOnFailure)

if __name__ == "__main__":
    main()
