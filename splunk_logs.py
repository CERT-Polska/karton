import os

import pika
import splunklib.client


service = splunklib.client.connect(host='localhost', port=8089, username='monk', password=os.environ['SPLUNK_PASSWORD'])
splunk_index = service.indexes['some_events']

connection = pika.BlockingConnection(pika.ConnectionParameters(host="localhost"))
channel = connection.channel()

result = channel.queue_declare(exclusive=True)
queue_name = result.method.queue

channel.queue_bind(exchange='karton.logs', queue=queue_name, routing_key='')


def log_callback(ch, method, properties, body):
    print('Sending', body)
    splunk_index.submit(event=body, source='logs', sourcetype='some_events')


channel.basic_consume(log_callback, queue=queue_name, no_ack=True)
channel.start_consuming()
