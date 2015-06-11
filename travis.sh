#!/bin/sh
# -e: fail on error
# -v: show commands
# -x: show expanded commands
set -vex

#env | sort
sudo mkdir -p /tmp
sudo chmod a+wrx /tmp
python setup.py install
nosetests --with-doctest -v src
