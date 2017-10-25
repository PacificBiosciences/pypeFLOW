WHEELHOUSE?=wheelhouse
PIP=pip wheel --wheel-dir ${WHEELHOUSE} --find-links ${WHEELHOUSE}
MY_TEST_FLAGS?=-v -s --durations=0

default:
pylint:
	pylint --errors-only pypeflow/ pwatcher/
pytest:
	python -c 'import pypeflow; print pypeflow'
	py.test ${MY_TEST_FLAGS} --junit-xml=test.xml --doctest-modules pypeflow/ pwatcher/
autopep8:
	autopep8 --max-line-length=120 -ir -j0 pypeflow/ pwatcher/
wheel:
	which pip
	${PIP} --no-deps .
	ls -larth ${WHEELHOUSE}
