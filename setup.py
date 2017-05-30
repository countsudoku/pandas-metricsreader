#!/usr/bin/env python
# -*- coding: utf-8 -*-

from setuptools import setup, find_packages

from pandas_metricsreader import __version__

setup(
    name='pandas-metricsreader',
    packages=find_packages(exclude=['docs', 'tests*']),
    version=__version__,
    description='Read data from different monitoring systems into a pandas DataFrame',
    author='Moritz C.K.U. Schneider',
    author_email='schneider.moritz@gmail.com',
    url='https://github.com/countsudoku/pandas-metricsreader',
    keywords=['graphite', 'pnp4nagios' ],
    install_requires=['pandas', 'requests'],
)
