import time

import redis

from minio import Minio

from karton.config import Config
from karton.housekeeper import KartonHousekeeper
from karton.resource import Resource


class Housekeeper(KartonHousekeeper):
    EXPIRATION_TIME = 3600*6
    identity = "karton.housekeeper"

    def __init__(self, *args, **kwargs):
        super(Housekeeper, self).__init__(*args, **kwargs)
        self.last_expiration_check = 0
        self.redis_config = dict(self.config.items("redis"))
        self.redis = redis.from_url(self.redis_config["url"])
        self.minio = Minio(self.config.minio_config["address"],
                           self.config.minio_config["access_key"],
                           self.config.minio_config["secret_key"],
                           secure=bool(int(self.config.minio_config.get("secure", True))))

    def process_expired(self):
        """
        Check if any of untracked resources expired
        """
        if time.time() > (self.last_expiration_check + self.EXPIRATION_TIME):
            self.log.info("Removing expired resources")
            for resource in self._get_expired_resources(self.EXPIRATION_TIME):
                resource._remove()
                self.log.debug("Removed {}".format(resource.uid))
            self.log.info("Removing expired resources done")
            self.last_expiration_check = time.time()

    def process_finished_task(self):
        """
        Handle finished tasks
        """
        if not self.redis.srem("subtasks_{}".format(self.task.root_uid), self.task.uid):
            self.log.warning("Out of sync - task {} is unknown".format(self.task.uid))
            return
        if not self.is_task_running(self.task.root_uid):
            self.log.info("Root task {} finished.".format(self.task.root_uid))
            # All tasks finished - perform cleanup
            for ruid in self.redis.smembers("resources_{}".format(self.task.root_uid)):
                bucket = self.redis.get("bucketof_{}".format(ruid))
                if bucket:
                    self._resource_object(bucket, ruid)._remove()
                    self.redis.delete("bucketof_{}".format(ruid))
                    self.log.debug("Removed {}".format(ruid))

                self.redis.delete("resources_{}".format(self.task.root_uid))

    def process_started_task(self):
        """
        Handle started tasks
        """
        if not self.is_task_running(self.task.root_uid):
            self.log.info("Root task {} started.".format(self.task.root_uid))

        self.redis.sadd("subtasks_{}".format(self.task.root_uid), self.task.uid)
        self.redis.expire("subtasks_{}".format(self.task.root_uid), self.EXPIRATION_TIME)

        for resource in self.task.resources:
            self.redis.set("bucketof_{}".format(resource.uid), resource.bucket)
            self.redis.expire("bucketof_{}".format(resource.uid), self.EXPIRATION_TIME)

            self.redis.sadd("resources_{}".format(self.task.root_uid), resource.uid)
            self.redis.expire("resources_{}".format(self.task.root_uid), self.EXPIRATION_TIME)

    def process(self):
        self.process_expired()

        if self.task_finished:
            self.process_finished_task()
        else:
            self.process_started_task()

    def _get_expired_resources(self, older_than):
        buckets = self.minio.list_buckets()
        for bucket in buckets:
            objects = self.minio.list_objects(bucket_name=bucket.name)
            for obj in objects:
                o = self.minio.stat_object(bucket_name=bucket.name, object_name=obj.object_name)
                minutes_since_now = ((time.mktime(time.gmtime()) - time.mktime(o.last_modified)) / 60)
                if minutes_since_now > older_than:
                    yield self._resource_object(bucket.name, obj.object_name)

    def _resource_object(self, bucket, uid):
        return Resource("resource", _uid=uid, bucket=bucket, config=self.config)

    def is_task_running(self, uid):
        return self.redis.exists("subtasks_{}".format(uid))


if __name__ == "__main__":
    conf = Config("config.ini")
    c = Housekeeper(conf)
    c.loop()
