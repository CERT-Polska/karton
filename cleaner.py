from minio import Minio
from karton import Config
import sys
import time

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Usage: python3 cleaner.py [older_than_minutes]")

    older_than_minutes = int(sys.argv[1])
    config = Config("config.ini").minio_config
    minio = Minio(config["address"],
                  config["access_key"],
                  config["secret_key"],
                  secure=bool(int(config.get("secure", True))))

    while True:
        buckets = minio.list_buckets()
        for bucket in buckets:
            objects = minio.list_objects(bucket_name=bucket.name)
            for obj in objects:
                o = minio.stat_object(bucket_name=bucket.name, object_name=obj.object_name)
                print(o.last_modified)
                print(time.gmtime())
                minutes_since_now = ((time.mktime(time.gmtime())-time.mktime(o.last_modified))/60)
                print(minutes_since_now)
                if minutes_since_now > older_than_minutes:
                    minio.remove_object(bucket_name=bucket.name, object_name=obj.object_name)
        time.sleep(older_than_minutes*60)





