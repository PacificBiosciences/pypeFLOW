#!/bin/bash -e

type module >& /dev/null || . /mnt/software/Modules/current/init/bash
module load python/2-UCS4

set -vex
which python
which pip

rm -rf LOCAL
mkdir -p LOCAL
export PYTHONUSERBASE=$(pwd)/LOCAL
export PATH=${PYTHONUSERBASE}/bin:${PATH}
WHEELHOUSE="/mnt/software/p/python/wheelhouse/develop/"

which pip
pip --version
pip -v install --user --no-index --find-links=${WHEELHOUSE} --edit .

export MY_TEST_FLAGS="-v -s --durations=0 --cov=. --cov-report=term-missing --cov-report=xml:coverage.xml --cov-branch"
make pytest
#sed -i -e 's@filename="@filename="./pypeflow/@g' coverage.xml
#sed -i -e 's@filename="@filename="./pwatcher/@g' coverage.xml

make pylint

pwd
ls -larth
find . -name '*.pyc' | xargs rm

# After testing, we can build and export the wheel.
bash bamboo_wheel.sh
