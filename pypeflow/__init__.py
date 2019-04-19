__version__ = '2.2.0' # should match setup.py

try:
    import sys, pkg_resources
    sys.stderr.write('{}\n'.format(pkg_resources.get_distribution('pypeflow')))
except Exception:
    pass
