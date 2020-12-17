# Karton <img src="img/logo.svg" width="64">

Distributed malware processing framework based on Python, Redis and MinIO.

## The idea

Karton is a framework designed for quickly creating analysis backends.
It allows you to build flexible malware* analysis pipelines and attach new systems with very little effort.

When dealing with the increasing number of threats we observe, we very often end up with a lot of scripts stuck together with duct tape and WD-40. These scripts are not production ready, but they do provide real value that should be stored and shared across systems for better threat processing.

We needed a solution that would allow us to very quickly deploy our PoC scripts and "insert" them into our analysis pipeline so that they could share their analysis results with other entities. And for this exact purpose, we created **Karton**.


*\* while Karton was designed with malware analysis in mind, it turned out that it performs quite nicely in other projects where microservice architecture is needed.*


## Installation

Installing the karton library is as easy as a single `pip install` command:

```
pip3 install karton-core
```

In order to setup the whole backend environment you will also need MinIO and Redis, see the [docs](https://karton-core.readthedocs.io/en/latest/getting_started.html#installation) for details.

## Example usage
To use karton you have to provide class that inherits from Karton.


```python
from karton.core import Karton, Task, Resource

class GenericUnpacker(Karton):
    """
    Performs sample unpacking
    """
    identity = "karton.generic-unpacker"
    filters = [
        {
            "type": "sample",
            "kind": "runnable",
            "platform": "win32"
        }
    ]

    def process(self, task: Task) -> None:
        # Get sample object
        packed_sample = task.get_resource('sample')
        # Log with self.log
        self.log.info(f"Hi {packed_sample.name}, let me analyze you!")
        ...
        # Send our results for further processing or reporting
        task = Task(
            {
               "type": "sample",
               "kind": "raw"
            }, payload = {
               "parent": packed_sample,
               "sample": Resource(filename, unpacked)
            })
        self.send_task(task)

if __name__ == "__main__":
    # Here comes the main loop
    GenericUnpacker().loop()
```

## Karton Systems

Since some karton systems are universal and will be useful to anyone running their own karton deployments, we've decided to open-source some of our repositories to the community.

#### [karton](https://github.com/CERT-Polska/karton)
This repository, contains `karton.system` service that acts as the main service responsible for dispatching tasks within the system and `karton.core` module that is used as the python library.

#### [karton-dashboard](https://github.com/CERT-Polska/karton-dashboard)
A small Flask application that allows for task and queue introspection.

#### [karton-classifier](https://github.com/CERT-Polska/karton-classifier)
Our main karton "router", it classifies unknown samples/files and produces various task types depending on the matched format. 

#### [karton-archive-extractor](https://github.com/CERT-Polska/karton-archive-extractor)
Generic archive unpacker that uses [sflock](https://github.com/hatching/sflock) internally.

#### [karton-config-extractor](https://github.com/CERT-Polska/karton-config-extractor)
Malware configuration extractor that tries to extract various embedded information from malware samples and analyses.

#### [karton-mwdb-reporter](https://github.com/CERT-Polska/karton-mwdb-reporter)
Analysis artifact reporter that submits all samples, tags, comments and relations between them to [MWDB](https://github.com/CERT-Polska/mwdb-core).

#### [karton-drakvuf](https://github.com/CERT-Polska/karton-drakvuf)
Malware sample processor that uploads incoming samples to [drakvuf-sandbox](https://github.com/CERT-Polska/drakvuf-sandbox) for analysis.

#### [karton-yaramatcher](https://github.com/CERT-Polska/karton-yaramatcher)
YARA classifier that spawns new tasks containing information about matched yara rules (rules not included ;)

#### [karton-asciimagic](https://github.com/CERT-Polska/karton-asciimagic)
Karton system that tries to extract executables (and other files) from various encodings like `hex`, `base64`, etc.

#### [karton-autoit-ripper](https://github.com/CERT-Polska/karton-autoit-ripper)
A small wrapper around [AutoIt-Ripper](https://github.com/nazywam/AutoIt-Ripper) that tries to extract embedded AutoIt scripts and resources from incoming executables.

#### [karton-misp-pusher](https://github.com/CERT-Polska/karton-misp-pusher)
A reporter that converts the artifacts to a MISP format and submits them as events.


Here is an example of how these systems could be linked together to create a basic malware analysis pipeline.
[![](img/karton-systems.svg)](img/karton-systems.svg?raw=true)
