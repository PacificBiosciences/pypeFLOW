#!/bin/bash
type module >& /dev/null || . /mnt/software/Modules/current/init/bash
module purge
module load gcc

set -vex
ls -larth ..
ls -larth
pwd

export WHEELHOUSE=./wheelhouse
mkdir -p ${WHEELHOUSE}

# Give everybody read/write access.
umask 0000


module load python/3.7.3
make wheel3

# http://bamboo.pacificbiosciences.com:8085/build/admin/edit/defaultBuildArtifact.action?buildKey=SAT-TAGDEPS-JOB1
# For old artifact config:
mkdir -p ./artifacts/gcc-6.4.0/wheelhouse
rsync -av ${WHEELHOUSE}/pypeflow*.whl artifacts/gcc-6.4.0/wheelhouse/


# Select export dir based on Bamboo branch, but only for develop and master.
case "${bamboo_planRepository_branchName}" in
  develop|master)
    WHEELHOUSE="/mnt/software/p/python/wheelhouse/${bamboo_planRepository_branchName}/"
    rsync -av ./wheelhouse/ ${WHEELHOUSE}
    ;;
  *)
    ;;
esac
