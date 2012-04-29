# -*- coding: utf-8 -*-

from distutils.core import setup
from distutils.extension import Extension

try:
    from Cython.Distutils import build_ext
except ImportError:
    use_cython = False
else:
    use_cython = True

cmdclass = {}
ext_modules = []

if use_cython:
    ext_modules = [
        Extension("jsondb._jsondb", ["jsondb/_jsondb.pyx"]),
    ]
    cmdclass.update({ 'build_ext': build_ext })
else:
    ext_modules = [
        Extension("jsondb._jsondb", ["jsondb/_jsondb.c"]),
    ]

requires = ()

import jsondb

setup(
    name='jsondb',
    version=jsondb.__version__,
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
    cmdclass=cmdclass,
    ext_modules=ext_modules,
)

