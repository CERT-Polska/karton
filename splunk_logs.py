"""
Fetch subsystem logs from RabbitMQ queue and feed them to Splunk.
"""
import argparse

import logging
import pika
import splunklib.client


logging.basicConfig(level=logging.INFO, format="[%(asctime)s][%(levelname)s] %(message)s")

parser = argparse.ArgumentParser()
parser.add_argument('--amqp-url', nargs='?', default='amqp://guest:guest@localhost')
parser.add_argument('--splunk-host', nargs='?', default='localhost')
parser.add_argument('--splunk-port', nargs='?', default=8089, type=int)
parser.add_argument('--splunk-username', nargs='?', required=True)
parser.add_argument('--splunk-password', nargs='?', required=True)

args = parser.parse_args()

service = splunklib.client.connect(host=args.splunk_host, port=args.splunk_port,
                                   username=args.splunk_username, password=args.splunk_password)
splunk_index = service.indexes['some_events']

connection = pika.BlockingConnection(pika.URLParameters(args.amqp_url))
channel = connection.channel()

result = channel.queue_declare(exclusive=True)
queue_name = result.method.queue

channel.queue_bind(exchange='karton.logs', queue=queue_name, routing_key='')
channel.queue_bind(exchange='karton.operations', queue=queue_name, routing_key='')


def log_callback(ch, method, properties, body):
    logging.info('Sending: %s', body)
    print(ch, method, properties)

    splunk_index.submit(event=body, source='logs', sourcetype='some_events')


channel.basic_consume(log_callback, queue=queue_name, no_ack=True)
channel.start_consuming()
