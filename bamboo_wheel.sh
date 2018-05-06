#!/bin/bash
type module >& /dev/null || . /mnt/software/Modules/current/init/bash
module purge
module load gcc

set -vex
ls -larth ..
ls -larth
pwd

export WHEELHOUSE=./wheelhouse

# Give everybody read/write access.
umask 0000


module load python/2-UCS2
make wheel

# For now, we have only "any" wheels, so we do not need to build again.

module unload python

module load python/2-UCS4
make wheel


# Select export dir based on Bamboo branch, but only for develop and master.
case "${bamboo_planRepository_branchName}" in
  develop|master)
    WHEELHOUSE="/mnt/software/p/python/wheelhouse/${bamboo_planRepository_branchName}/"
    ;;
  *)
    #WHEELHOUSE="/home/UNIXHOME/cdunn/wheelhouse/gcc-6/"
    WHEELHOUSE="./wheelhouse/"
    mkdir -p ${WHEELHOUSE}
    ;;
esac

# http://bamboo.pacificbiosciences.com:8085/build/admin/edit/defaultBuildArtifact.action?buildKey=SAT-TAGDEPS-JOB1
# For old artifact config:
mkdir -p ./artifacts/gcc-6.4.0/wheelhouse
rsync -av ${WHEELHOUSE}/pypeflow*.whl artifacts/gcc-6.4.0/wheelhouse/

rsync -av ./wheelhouse/ ${WHEELHOUSE}
