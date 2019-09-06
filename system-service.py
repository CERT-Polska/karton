import json
import time
import uuid

import splunklib.client

import minio

from karton.base import KartonBase
from karton.resource import Resource, RemoteResource
from karton import Config


class SystemServiceConfig(Config):
    DEFAULT_BUCKET_EXPIRATION = 3600 * 24
    DEFAULT_CHECK_INTERVAL = 300
    DEFAULT_STATUS_EXPIRATION = 3600

    def __init__(self, path):
        super(SystemServiceConfig, self).__init__(path)
        self.buckets_config = dict(self.config.items("buckets") if self.config.has_section("buckets") else [])
        self.system_service_config = dict(self.config.items("system_service")
                                       if self.config.has_section("system_service") else [])
        self.splunk_config = dict(self.config.items("splunk")
                                  if self.config.has_section("splunk") else [])
        self.expiration_check_interval = int(self.system_service_config.get("check_interval",
                                             SystemServiceConfig.DEFAULT_CHECK_INTERVAL))
        self.default_bucket_expiration = int(self.system_service_config.get("bucket_expiration",
                                             SystemServiceConfig.DEFAULT_BUCKET_EXPIRATION))

    def bucket_expiration(self, bucket_name):
        return int(self.buckets_config.get(bucket_name, self.default_bucket_expiration))


class SystemService(KartonBase):
    identity = "karton.system"

    def __init__(self, config):
        super(SystemService, self).__init__(config=config)
        self.last_expiration_check = 0
        splunk = splunklib.client.connect(host=self.config.splunk_config["host"],
                                          port=self.config.splunk_config["port"],
                                          username=self.config.splunk_config["username"],
                                          password=self.config.splunk_config["password"],
                                          scheme=self.config.splunk_config["scheme"],
                                          autologin=True)
        self.splunk_index = splunk.indexes['karton']

    def process_expired(self):
        """
        Check if any of untracked resources expired
        """
        if time.time() > (self.last_expiration_check + self.config.expiration_check_interval):
            self.log.info("Removing expired resources")
            try:
                buckets = self.minio.list_buckets()
                for bucket in buckets:
                    older_than = self.config.bucket_expiration(bucket.name)
                    objects = self.minio.list_objects(bucket_name=bucket.name)
                    for obj in objects:
                        try:
                            # TODO query splunk (as in karton-status) to determine whether analysis is pending
                            o = self.minio.stat_object(bucket_name=bucket.name, object_name=obj.object_name)
                            seconds_since_now = time.mktime(time.gmtime()) - time.mktime(o.last_modified)
                            if seconds_since_now > older_than:
                                resource = self._resource_object(bucket.name, obj.object_name)
                                resource.remove(self.minio)
                                self.log.info("Removed resource {}:{}".format(bucket.name, obj.object_name))
                        except minio.error.NoSuchKey:
                            self.log.warn("Couldn't remove object: %s" % obj.object_name)
            except Exception as e:
                self.log.exception("Error during removing expired resources")
            self.log.info("Removing expired resources done")
            self.last_expiration_check = time.time()

    def _resource_object(self, bucket, uid):
        return RemoteResource("resource", _uid=uid, bucket=bucket)

    def internal_process(self, queue, body):
        if queue == 'karton.tasks':
            body = json.loads(body)
            headers = body["headers"]
            bound_identities = set()

            for client in self.rs.client_list():
                bound_identities.add(client["name"])

            for identity, raw_binds in self.rs.hgetall('karton.binds').items():
                binds = json.loads(raw_binds)

                if identity not in bound_identities:
                    self.log.info('Unbound identity detected: {}'.format(identity))
                    pipe = self.rs.pipeline()
                    pipe.lrange(identity, 0, -1)
                    pipe.delete(identity)
                    results = pipe.execute()

                    for raw_task in results[0]:
                        self.splunk_index.submit(event=json.dumps({
                            "status": "Failed",
                            "identity": identity,
                            "task": json.loads(raw_task),
                            "type": "operation"
                        }))

                    self.rs.hdel("karton.binds", identity)
                    break

                for bind in binds:
                    if all(headers.get(bind_key) == bind_value for bind_key, bind_value in bind.items()):
                        self.log.info('Task matched binds for {}'.format(identity))

                        # TODO do this in more civilized way?
                        body["uid"] = str(uuid.uuid4())
                        self.splunk_index.submit(event=json.dumps({
                            "status": "Spawned",
                            "identity": identity,
                            "task": body,
                            "type": "operation"
                        }))
                        self.rs.lpush(identity, json.dumps(body))
        elif queue == 'karton.logs' or queue == 'karton.operations':
            try:
                if not isinstance(body, str):
                    body = body.decode("utf8")
                body = json.loads(body)
                if "task" in body and isinstance(body["task"], str):
                    body["task"] = json.loads(body["task"])
                self.splunk_index.submit(event=json.dumps(body))
            except Exception as e:
                import traceback
                traceback.print_exc()

    def loop(self):
        self.log.info("Manager {} started".format(self.identity))

        while True:
            # order does matter! task dispatching must be before karton.operations to avoid races
            data = self.rs.brpop(['karton.logs', 'karton.tasks', 'karton.operations'], timeout=30)

            if data:
                queue, body = data
                self.internal_process(queue, body)

            try:
                self.process_expired()
            except Exception as e:
                import traceback
                traceback.print_exc()


if __name__ == "__main__":
    conf = SystemServiceConfig("config.ini")
    c = SystemService(conf)
    c.loop()
