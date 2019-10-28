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
    package_data={"": ["karton/templates/*"]},
    include_package_data=True,
    install_requires=open("requirements.txt").read().splitlines(),
    scripts=["karton/kpm"],
    classifiers=[
        "Programming Language :: Python",
        "Operating System :: OS Independent",
    ],
)
