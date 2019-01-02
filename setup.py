from setuptools import setup, Extension, find_packages
import subprocess

try:
    local_version = '+git.{}'.format(
        subprocess.check_output('git rev-parse HEAD', shell=True))
except Exception:
    local_version = ''

setup(
    name = 'pypeflow',
    version='2.1.1' + local_version,
    author='J. Chin',
    author_email='cschin@infoecho.net',
    license='LICENSE.txt',
    packages=find_packages(),
    package_dir = {'':'.'},
    zip_safe = False,
    install_requires=[
        'networkx >=1.9.1',
        'future >= 0.16.0',
    ],
    entry_points = {'console_scripts': [
            'pwatcher-main=pwatcher.mains.pwatcher:main',
            'pwatcher-pypeflow-example=pwatcher.mains.pypeflow_example:main',
            'heartbeat-wrapper=pwatcher.mains.fs_heartbeat:main',
        ],
    },
    package_data={'pwatcher.mains': ['*.sh']}
)
