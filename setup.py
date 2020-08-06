#!/usr/bin/env python
try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

import os

version_path = os.path.join(os.path.dirname(__file__), "karton/__version__.py")
version_info = {}
with open(version_path) as f:
    exec(f.read(), version_info)

setup(
    name="karton2",
    version=version_info["__version__"],
    description="Base library for karton subsystems",
    package_dir={"karton2": "karton"},
    packages=["karton2", "karton2.services"],
    install_requires=open("requirements.txt").read().splitlines(),
    extras_require={
        ':python_version < "3"': [
            'mock==3.0.5'
        ]
    },
    entry_points={
        'console_scripts': [
            'karton-system=karton2.services.system:main',
            'karton=karton2.main:main'
        ],
    },
    classifiers=[
        "Programming Language :: Python",
        "Operating System :: OS Independent",
    ],
)
