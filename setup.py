#!/usr/bin/env python

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

setup(name="karton",
      version="1.3.3a",
      description="Base library for karton subsystems",
      package_dir={'karton': 'karton'},
      packages=['karton'],
      install_requires=open("requirements.txt").read().splitlines(),
      classifiers=[
          "Programming Language :: Python",
          "Operating System :: OS Independent",
      ])
