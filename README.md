# libkarton

Distributed malware processing framework based on Python, RabbitMQ, Splunk and Minio.

### The idea
With increasing number of threats we observe together with the speed in which they change and evolve we struggle in deploying solutions in time.
Many times we have some sort of scripts made with duct tape and WD-40. Those scripts are surely not production ready, but give real value that should be stored and shared across systems for better processing.

We need a solution for fast deployment of PoC scripts, so they can share their results with other entities.
That's why we are developing Karton, wrapper for your package! :D
### Installation
```
pip3 install git+ssh://git@vcs.cert.pl/karton/karton.git
```

or using docker: ...

### Usage
To use karton you have to provide class that inherits from Karton.


```python
    class Classifier(Karton):
    # identity is used for unique identification of the given subsystem
    # it will also become a queue name after launch
    # for horizontal scaling, you can reuse this in multiple instances
    identity = "karton.classifier"
    
    # filters define what you want to get from the queue
    # key: value pairs are AND'ed and list elements are OR'ed
    filters = [
        {
            "type": "sample",
            "kind": "raw"
        },
    ]

    def process(self):
        # self.current_task - stores task which arrival invoked the process() function
        # self.current_task.headers - dict of headers, useful when multiple filters are used
        
        # get_resource() gets remote resource object
        remote_sample = self.current_task.get_resource("sample")
        
        # download_resource actually downloads content from remote resource
        sample = self.current_task.download_resource(remote_sample)
        
        # you can access content of resources for processing
        sample_content = sample.content
        print(sample_content)
        
        # process
        # handling of task should either end in creating new task
        # or adding some newly obtained information to one of the 
        # persistent storage systems, ie. mwdb
        if sample_content[:2] == b"MZ":
            # derive task from current task, this saves resources of current_task
            task = Task.derive_task({"type": "sample", "kind": "executable"}, self.current_task)
            # send task to queues for further processing
            self.send_task(task)
```

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
