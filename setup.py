#!/usr/bin/env python

from setuptools import setup

setup(name='b2luigi',
      version='0.0.1',
      description='Luigi for Basf2',
      author='Nils Braun',
      author_email='nils.braun@kit.edu',
      packages=['b2luigi'],
      setup_requires=['pytest-runner'],
      tests_require=['pytest'],
     )
