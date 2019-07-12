#!/usr/bin/env python

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

setup(
    name="karton",
    version="1.3.5",
    description="Base library for karton subsystems",
    package_dir={"karton": "karton"},
    packages=["karton"],
    install_requires=open("requirements.txt").read().splitlines(),
    scripts=['karton/kpm'],
    classifiers=[
        "Programming Language :: Python",
        "Operating System :: OS Independent",
    ],
)
