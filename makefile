default:
	nosetests -v --with-doctest pypeflow/ pwatcher/fs_based.py
pylint:
	pylint --errors-only pypeflow/ pwatcher/
