```
Usage: sbatch [OPTIONS...] executable [args...]

Parallel run options:
  -a, --array=indexes         job array index values
  -A, --account=name          charge job to specified account
      --bb=<spec>             burst buffer specifications
      --begin=time            defer job until HH:MM MM/DD/YY
  -M, --clusters=names        Comma separated list of clusters to issue
                              commands to.  Default is current cluster.
                              Name of 'all' will submit to run on all clusters.
      --comment=name          arbitrary comment
      --cpu-freq=min[-max[:gov]] requested cpu frequency (and governor)
  -c, --cpus-per-task=ncpus   number of cpus required per task
  -d, --dependency=type:jobid defer job until condition on jobid is satisfied
  -D, --workdir=directory     set working directory for batch script
  -e, --error=err             file for batch script's standard error
      --export[=names]        specify environment variables to export
      --export-file=file|fd   specify environment variables file or file
                              descriptor to export
      --get-user-env          load environment from local cluster
      --gid=group_id          group ID to run job as (user root only)
      --gres=list             required generic resources
  -H, --hold                  submit job in held state
      --ignore-pbs            Ignore #PBS options in the batch script
  -i, --input=in              file for batch script's standard input
  -I, --immediate             exit if resources are not immediately available
      --jobid=id              run under already allocated job
  -J, --job-name=jobname      name of job
  -k, --no-kill               do not kill job on node failure
  -L, --licenses=names        required license, comma separated
  -m, --distribution=type     distribution method for processes to nodes
                              (type = block|cyclic|arbitrary)
      --mail-type=type        notify on state change: BEGIN, END, FAIL or ALL
      --mail-user=user        who to send email notification for job state
                              changes
  -n, --ntasks=ntasks         number of tasks to run
      --nice[=value]          decrease scheduling priority by value
      --no-requeue            if set, do not permit the job to be requeued
      --ntasks-per-node=n     number of tasks to invoke on each node
  -N, --nodes=N               number of nodes on which to run (N = min[-max])
  -o, --output=out            file for batch script's standard output
  -O, --overcommit            overcommit resources
  -p, --partition=partition   partition requested
      --parsable              outputs only the jobid and cluster name (if present),
                              separated by semicolon, only on successful submission.
      --power=flags           power management options
      --priority=value        set the priority of the job to value
      --profile=value         enable acct_gather_profile for detailed data
                              value is all or none or any combination of
                              energy, lustre, network or task
      --propagate[=rlimits]   propagate all [or specific list of] rlimits
      --qos=qos               quality of service
  -Q, --quiet                 quiet mode (suppress informational messages)
      --reboot                reboot compute nodes before starting job
      --requeue               if set, permit the job to be requeued
  -s, --share                 share nodes with other jobs
  -S, --core-spec=cores       count of reserved cores
      --sicp                  If specified, signifies job is to receive
      --signal=[B:]num[@time] send signal when time limit within time seconds
      --switches=max-switches{@max-time-to-wait}
                              Optimum switches and max time to wait for optimum
      --thread-spec=threads   count of reserved threads
  -t, --time=minutes          time limit
      --time-min=minutes      minimum time limit (if distinct)
      --uid=user_id           user ID to run job as (user root only)
  -v, --verbose               verbose mode (multiple -v's increase verbosity)
      --wckey=wckey           wckey to run job under
      --wrap[=command string] wrap command string in a sh script and submit

Constraint options:
      --contiguous            demand a contiguous range of nodes
  -C, --constraint=list       specify a list of constraints
  -F, --nodefile=filename     request a specific list of hosts
      --mem=MB                minimum amount of real memory
      --mincpus=n             minimum number of logical processors (threads)
                              per node
      --reservation=name      allocate resources from named reservation
      --tmp=MB                minimum amount of temporary disk
  -w, --nodelist=hosts...     request a specific list of hosts
  -x, --exclude=hosts...      exclude a specific list of hosts

Consumable resources related options:
      --exclusive[=user]      allocate nodes in exclusive mode when
                              cpu consumable resource is enabled
      --mem-per-cpu=MB        maximum amount of real memory per allocated
                              cpu required by the job.
                              --mem >= --mem-per-cpu if --mem is specified.

Affinity/Multi-core options: (when the task/affinity plugin is enabled)
  -B  --extra-node-info=S[:C[:T]]            Expands to:
       --sockets-per-node=S   number of sockets per node to allocate
       --cores-per-socket=C   number of cores per socket to allocate
       --threads-per-core=T   number of threads per core to allocate
                              each field can be 'min' or wildcard '*'
                              total cpus requested = (N x S x C x T)

      --ntasks-per-core=n     number of tasks to invoke on each core
      --ntasks-per-socket=n   number of tasks to invoke on each socket


Help options:
  -h, --help                  show this help message
  -u, --usage                 display brief usage message

Other options:
  -V, --version               output version information and exit
```

* https://github.com/PacificBiosciences/FALCON-integrate/issues/53
* http://slurm.schedmd.com/
* http://slurm.schedmd.com/sbatch.html
