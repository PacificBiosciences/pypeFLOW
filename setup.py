from setuptools import setup, Extension, find_packages

setup(
    name = 'pypeflow',
    version='1.0.0',
    author='J. Chin',
    author_email='cschin@infoecho.net',
    license='LICENSE.txt',
    packages = [
        'pypeflow',
        'pwatcher', # a separate package here for convenience, for now
        'pwatcher.mains',
    ],
    package_dir = {'':'.'},
    zip_safe = False,
    install_requires=[
        'networkx >=1.7, <=1.11',
    ],
    entry_points = {'console_scripts': [
            'pwatcher-main=pwatcher.mains.pwatcher:main',
            'pwatcher-pypeflow-example=pwatcher.mains.pypeflow_example:main',
            'heartbeat-wrapper=pwatcher.mains.fs_heartbeat:main',
        ],
    },
    package_data={'pwatcher.mains': ['*.sh']}
)
