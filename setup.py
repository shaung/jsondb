#!/usr/bin/env python
# -*- coding: utf-8 -*-

__version__ = '0.1.1'

from distutils.core import setup

requires = ()

setup(
    name='jsondb',
    version=__version__,
    url='https://github.com/shaung/jsondb/',
    download_url='http://pypi.python.org/pypi/jsondb',
    license='BSD',
    author='shaung',
    author_email='shaun.geng@gmail.com',
    description='JSON file as a database',
    long_description=open('README.md').read(),
    zip_safe=False,
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Topic :: Database',
        'Topic :: Text Processing :: Markup',
    ],
    platforms='any',
    install_requires=requires,
    packages=[
        'jsondb',
        'jsondb.backends',
    ],
)

