WHEELHOUSE?=wheelhouse
PIP=pip wheel --wheel-dir ${WHEELHOUSE} --find-links ${WHEELHOUSE}

default:
	nosetests -v --with-doctest pypeflow/ pwatcher/fs_based.py
pylint:
	pylint --errors-only pypeflow/ pwatcher/
wheel:
	which pip
	${PIP} --no-deps .
	ls -larth ${WHEELHOUSE}
