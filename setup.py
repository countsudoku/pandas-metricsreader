#!/usr/bin/env python
# -*- coding: utf-8 -*-

from setuptools import setup, find_packages
from ast import parse
import os


def version():
    """Return version string."""
    with open(os.path.join(os.path.abspath(os.path.dirname(__file__)),
                           'pandas_metricsreader',
                           '__init__.py')) as input_file:
        for line in input_file:
            if line.startswith('__version__'):
                return parse(line).body[0].value.s

setup(
    name='pandas-metricsreader',
    packages=find_packages(exclude=['docs', 'tests*']),
    version=version(),
    description='Read data from different monitoring systems into a pandas DataFrame',
    author='Moritz C.K.U. Schneider',
    author_email='schneider.moritz@gmail.com',
    url='https://github.com/countsudoku/pandas-metricsreader',
    keywords=['graphite', 'pnp4nagios' ],
    install_requires=['pandas', 'requests'],
)
