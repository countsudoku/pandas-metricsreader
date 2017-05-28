#!/usr/bin/env python
# -*- coding: utf-8 -*-

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

from pandas_metricsreader import __version__

setup(
    name='pandas-metricsreader',
    packages=['pandas-metricsreader',],
    version=__version__,
    description='Read data from different monitoring systems into a pandas DataFrame',
    author='Moritz C.K.U. Schneider',
    author_email='schneider.moritz@gmail.com',
    url='https://github.com/countsudoku/pandas-metricsreader',
    keywords=['graphite', 'pnp4nagios' ],
    license='BSD License',
    install_requires=['pandas', 'requests'],
)
