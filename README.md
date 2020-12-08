# Karton

![LOGO](/logo/logo.png)

Distributed malware processing framework based on Python, Redis, Splunk and MinIO.

### The idea

Karton is a library made for analysis backend orchestration.
Allows you to build flexible malware analysis pipelines and attach new systems with ease.

With increasing number of threats we observe together with the speed in which they change and evolve we struggle in deploying solutions in time.
Many times we have some sort of scripts made with duct tape and WD-40. Those scripts are surely not production ready, but give real value that should be stored and shared across systems for better processing.

We need a solution for fast deployment of PoC scripts, so they can share their results with other entities.
That's why we are developing Karton, wrapper for your package! :)

### Installation

```
pip install karton-system
```

### Usage
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
