## pwatcher
Filesystem-based process-watcher.

Sometimes, the filesystem is the only reliable way to communicate between
processes on different machines.  **pwatcher** will watch for
sentinels and heartbeats.

Two basic ideas:

1. To store sentinel-files in a single directory, in order to reduce
   the burden on the filesystem.
2. To use a background thread to update heartbeat-files periodically,
   in order to avoid waiting forever on dead jobs.

## API
**pwatcher** can be used as a separate process or as a Python module.
If you use it as a module, you should use the contextmanager in order
to release locks quickly. That way, users can query via the command-line
even while a large job is ongoing.

There are three commands in the API.

1. `run`
2. `query`
3. `delete`

They all can be called from the command-line by supplying the arguments as JSON.

### Examples
#### Using **pwatcher**
```
pip install -e .
cd examples-pwatcher/ab
pwatcher-main run < run.json
pwatcher-main query < query.json
pwatcher-main delete < delete.json
ls pwatched/
```
#### pypeFLOW example
To run this example, you must first install **pypeFLOW**.
```
mkdir foo
cd foo
pypeflow_example
```
That should create:
* directory `mytmp`
  * for pypeflow outputs
* directory `watched`
  * `state.py`
  * wrappers
  * sentinel-files, touched on exit
  * heartbeat-files, usually removed when done
* some basic taskrunners

### Plans
The API needs a bit of clean-up, but the basic functionality is there.
I still have to inject the grid-control commands.

I hope to replace **FALCON**'s `fc_run.py` soon!
