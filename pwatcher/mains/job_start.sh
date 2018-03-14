#!/bin/bash
# vim: filetype=sh ts=4 sw=4 sts=4 et:
#
# Wait until file exists, then spawn.

# This is not Python because the start_tmpl from pbsmrtpipe always runs bash.
# But we use the .py extension because we want this installed with our Python
# code, so we do not need to deal with mobs for installation. (But we might
# need to chmod +x.)
#
# This can be run via
#
#     bash -c pwatcher/mains.job_start.py myprog 60
#
# Note: If anyone replaces this, you must ensure that running this is exactly equivalent
# to running the "executable". In other words, no 'mkdir', no 'cd', etc. That will help
# with debugging.

set -vex
executable=${PYPEFLOW_JOB_START_SCRIPT}
timeout=${PYPEFLOW_JOB_START_TIMEOUT:-60} # wait 60s by default

# Wait up to timeout seconds for the executable to become "executable",
# then exec.
#timeleft = int(timeout)
while [[ ! -x "${executable}" ]]; do
    if [[ "${timeout}" == "0" ]]; then
        echo "timed out waiting for (${executable})"
        exit 77
    fi
    echo "not executable: '${executable}', waiting ${timeout}s"
    sleep 1
    timeout=$((timeout-1))
done

/bin/bash ${executable}
