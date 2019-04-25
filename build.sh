#!/bin/bash -e

type module >& /dev/null || . /mnt/software/Modules/current/init/bash
module load python/3.7.3

set -vex
which python3
which pip3

export PYTHONUSERBASE=$(pwd)/.git/LOCAL
export PATH=${PYTHONUSERBASE}/bin:${PATH}
WHEELHOUSE="/mnt/software/p/python/wheelhouse/develop/"

rm -rf ${PYTHONUSERBASE}
mkdir -p ${PYTHONUSERBASE}

pip3 --version
pip3 install --user pylint
pip3 install --user --find-links=${WHEELHOUSE} pytest pytest-cov
pip3 install --user --find-links=${WHEELHOUSE} --edit .
# cannot use --no-index b/c so many py3 pkgs are not wheelable

export MY_TEST_FLAGS="-v -s --durations=0 --cov=. --cov-report=term-missing --cov-report=xml:coverage.xml --cov-branch"
make pytest
#sed -i -e 's@filename="@filename="./pypeflow/@g' coverage.xml
#sed -i -e 's@filename="@filename="./pwatcher/@g' coverage.xml

make pylint

pwd
ls -larth
find . -name '*.pyc' | xargs rm

# After testing, we can build and export the wheel.
#bash bamboo_wheel.sh ###################################################
