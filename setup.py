#!/usr/bin/env python

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

setup(
    name="karton2",
    version="2.0.3",
    description="Base library for karton subsystems",
    package_dir={"karton2": "karton"},
    packages=["karton2"],
    package_data={"": ["karton/templates/*"]},
    include_package_data=True,
    install_requires=open("requirements.txt").read().splitlines(),
    scripts=["karton/kpm"],
    classifiers=[
        "Programming Language :: Python",
        "Operating System :: OS Independent",
    ],
)
