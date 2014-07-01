#!/usr/bin/env python

from setuptools import setup, find_packages


install_description = '''
scouchdb
=====

Python library for couchdb

based on pycouchdb

Installation
------------

You can get the code from `https://github.com/GuoJing/scouchdb`

'''

setup(
    name='scouchdb',
    version='0.0.1',
    license='GPLv3',
    description='Python library for couchdb, based on pycouchdb',
    long_description=install_description,
    author='GuoJing',
    author_email='soundbbg@gmail.com',
    url='http://github.com/GuoJing/scouchdb',
    packages=find_packages(exclude=['ez_setup', 'examples', 'tests']),
    include_package_data=True,
    zip_safe=False,
    classifiers=[
        'Development Status :: 3 - Alpha',
        'License :: OSI Approved :: GNU General Public License v3 (GPLv3)',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.7',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ],
    install_requires=[
        'pycouchdb',
    ],
)
