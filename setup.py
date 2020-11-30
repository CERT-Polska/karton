#!/usr/bin/env python
try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

import os

version_path = os.path.join(os.path.dirname(__file__), "karton/core/__version__.py")
version_info = {}
with open(version_path) as f:
    exec(f.read(), version_info)

setup(
    name="karton-core",
    version=version_info["__version__"],
    description="Distributed malware analysis orchestration framework",
    namespace_packages=["karton"],
    packages=["karton.core", "karton.system"],
    install_requires=open("requirements.txt").read().splitlines(),
    extras_require={
        ':python_version < "3"': [
            'mock==3.0.5'
        ]
    },
    entry_points={
        'console_scripts': [
            'karton-system=karton.system:SystemService.main',
            'karton=karton.core.main:main'
        ],
    },
    classifiers=[
        "Programming Language :: Python",
        "Operating System :: OS Independent",
    ],
)
