# libkarton

Distributed malware processing framework based on Python, RabbitMQ, Splunk and Minio.

### The idea
With increasing number of threats we observe together with the speed in which they change and evolve we struggle in deploying solutions in time.
Many times we have some sort of scripts made with duct tape and WD-40. Those scripts are surely not production ready, but give real value that should be stored and shared across systems for better processing.

We need a solution for fast deployment of PoC scripts, so they can share their results with other entities.
That's why we are developing Karton, wrapper for your package! :D

### Usage
To use karton you have to provide class that inherits from Karton.

TODO(des): dopisac rzeczy

### Technical details
The above is accomplished by strapping together RabbitMQ with Minio.

RMQ is used for broadcast of tasks in producer-consumer model.

Minio is used as a hub for sharing resources that are too big to be handled by RMQ.

#### Example

**We added a sample to the mwdb.** - This gives us 2 categories of information.

- Metadata
- Resource

In this case metadata can be things like "this is vbs", "this sample has tag `danabot`" or sha256 of the contents.

For resources, we have just "sample", which contains... sample. There can be more resources than one.
