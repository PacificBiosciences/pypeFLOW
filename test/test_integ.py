from pypeflow.simple_pwatcher_bridge import (
    PypeProcWatcherWorkflow,
    PRODUCERS,
)
from pypeflow import sample_tasks
from pypeflow import util
import os

def setup_workflow():
    PRODUCERS.clear() # Forget any PypeTasks already defined.

    config = {
        'job_type': 'string',
        #'job_queue': 'bash -C ${CMD} >| ${STDOUT_FILE} 2>| ${STDERR_FILE}',
        'job_queue': 'bash -C ${CMD}',
        #'sge_option': '-pe smp 8 -q bigmem',
        'pwatcher_type': 'blocking',
        #watcher_directory=config.get('pwatcher_directory', 'mypwatcher'),
        #'use_tmpdir': '/scratch',
    }
    wf = PypeProcWatcherWorkflow(
        max_jobs=4,
        job_type=config['job_type'],
        job_queue=config.get('job_queue'),
        sge_option=config.get('sge_option'),
        watcher_type=config.get('pwatcher_type'),
        #watcher_directory=config.get('pwatcher_directory', 'mypwatcher'),
        #use_tmpdir=config.get('use_tmpdir'),
    )
    return wf

def try_workflow(text, create_task):
    """Test the whole workflow.
    'text' is anything.
    'create_tasks' signature: create_task(i1, o1)
    """
    wf = setup_workflow()
    wf.max_jobs = 2

    i1 = './in/i1'
    o1 = './run/dir1/o1.txt'
    util.mkdirs('in/')
    with open('in/i1', 'w') as ofs:
        ofs.write(text)
    assert os.path.exists(i1)
    assert not os.path.exists(o1)

    task = create_task(i1, o1)
    wf.addTask(task)
    wf.refreshTargets()

    assert os.path.exists(o1)
    assert text == open(o1).read()

def test_old(tmpdir):
    with tmpdir.as_cwd():
        try_workflow('OLD', sample_tasks.create_task_old)

def test_new(tmpdir):
    with tmpdir.as_cwd():
        try_workflow('NEW', sample_tasks.create_task_new)
