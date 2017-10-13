#!/bin/bash -e

type module >& /dev/null || . /mnt/software/Modules/current/init/bash
module load python/2.7.13-UCS4

set -vex
which python
which pip

mkdir -p LOCAL
export PYTHONUSERBASE=$(pwd)/LOCAL
export PATH=${PYTHONUSERBASE}/bin:${PATH}

pip -v install --user --edit .

pip install --user pytest pytest-cov pylint

export MY_TEST_FLAGS="-v -s --durations=0 --cov=. --cov-report=term-missing --cov-report=xml:coverage.xml --cov-branch"
make pytest
#sed -i -e 's@filename="@filename="./pypeflow/@g' coverage.xml
#sed -i -e 's@filename="@filename="./pwatcher/@g' coverage.xml

make pylint

pwd
ls -larth
find . -name '*.pyc' | xargs rm
