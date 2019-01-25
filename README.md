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

    class Classifier(Karton):
    # identity is used for unique idefntification of subsystem
    # it will also become a queue name when started
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

        sample = None
        # current_task contains resources which are needed to handle the task
        for resource in self.current_task.resources:
            if resource.name == "sample":
                sample = resource
                break
        else:
            return
        
        # you can access content of resources for processing
        sample_content = sample.content
        print(sample_content)
        
        # process
        # handling of task should either end in creating new task
        # or adding some newly obtained information to one of the 
        # persistent storage systems, ie. mwdb
        if sample_content[:2] == b"MZ":
            # create new task if needed
            task = self.create_task({"type": "sample", "kind": "executable"})
            # append resource to task
            task.add_resource(sample)
            # send task to queues for further processing
            self.send_task(task)

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
