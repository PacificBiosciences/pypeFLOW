try:
    import sys, pkg_resources
    sys.stderr.write('{}\n'.format(pkg_resources.get_distribution('pypeflow')))
except Exception:
    pass
