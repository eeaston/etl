#!/usr/bin/env python
import os
from codecs import open

from setuptools import setup, find_packages

here = os.path.dirname(__file__)
if here:
    os.chdir(here)

with open('README.rst', 'r', 'utf-8') as f:
    readme = f.read()
with open('CHANGES.rst', 'r', 'utf-8') as f:
    changes = f.read()

requires = [
    'path.py',
    'docopt',
    'schematics',
    'rethinkdb',
]

tests_require = [
    'pkglib_testing',
]


setup(
    name='etl',
    version='0.9.0',
    description='ETL Framework for Python',
    long_description=readme + '\n\n' + changes,
    author='Edward Easton',
    author_email='eeaston@gmail.com',
    url='http://github.com/eeaston/etl',
    packages=find_packages(),
    package_data={'': ['LICENSE']},
    include_package_data=True,
    install_requires=requires,
    tests_require=tests_require,
    license='MIT',
    zip_safe=True,
    classifiers=(
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.7',
        'Topic :: Software Development :: Libraries',
        'Topic :: Database',
    ),
    extras_require={
    },
    entry_points={
        'console_scripts': [
        #    'test_csv_collector = etl.scripts.run_test_csv_collector:main'
        ],
    }
)
