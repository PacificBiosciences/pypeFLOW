from setuptools import setup, Extension, find_packages

setup(
    name = 'pypeflow',
    version='0.1.1',
    author='J. Chin',
    author_email='cschin@infoecho.net',
    license='LICENSE.txt',
    packages = find_packages('src'), 
    package_dir = {'':'src'},
    zip_safe = False,
    install_requires=[
        'rdflib == 3.4.0',
        'rdfextras >= 0.1'
    ]
    )
