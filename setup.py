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
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    namespace_packages=["karton"],
    packages=["karton.core", "karton.system"],
    package_data={"karton.core": ["py.typed"]},
    install_requires=open("requirements.txt").read().splitlines(),
    entry_points={
        'console_scripts': [
            'karton-system=karton.system:SystemService.main',
            'karton=karton.core.main:main'
        ],
    },
    url="https://github.com/CERT-Polska/karton",
    classifiers=[
        "Programming Language :: Python",
        "Operating System :: OS Independent",
    ],
)
