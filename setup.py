#!/usr/bin/env python

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

setup(
    name="karton2",
    version="2.2.0",
    description="Base library for karton subsystems",
    package_dir={"karton2": "karton"},
    packages=["karton2"],
    package_data={"": ["karton/templates/*"]},
    include_package_data=True,
    install_requires=open("requirements.txt").read().splitlines(),
    extras_require={
        ':python_version < "3"': [
            'mock==3.0.5'
        ]
    },
    scripts=["karton/kpm"],
    classifiers=[
        "Programming Language :: Python",
        "Operating System :: OS Independent",
    ],
)
