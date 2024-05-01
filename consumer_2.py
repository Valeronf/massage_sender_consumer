import time
import pika
import random
from pika.exchange_type import ExchangeType

def get_replay(ch, method, properties, body):
    if method.routing_key == 'items.type2':
        info = body.decode('utf-8')
        print(f"received message: {info}")
        time.sleep(3)
        ch.basic_ack(delivery_tag=method.delivery_tag)

def dlx_received(ch, method, properties, body):
    if method.routing_key == 'items.type2':
        info = body.decode('utf-8')
        print(f"sent to dlx{info}")
        ch.basic_ack(delivery_tag=method.delivery_tag)

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

channel.exchange_declare(exchange='exch_topic', exchange_type=ExchangeType.topic)
channel.exchange_declare(exchange='dlx', exchange_type=ExchangeType.fanout)
channel.basic_qos(prefetch_count=1)

# Оголошення черги та прив'язка до обмінника
channel.queue_declare(queue='get_replay', arguments={'x-dead-letter-exchange': 'dlx'})
channel.queue_bind(queue='get_replay', exchange='exch_topic', routing_key='items.type2')
channel.basic_consume(queue='get_replay', auto_ack=False, on_message_callback=get_replay)

# Оголошення черги для Dead Letter Queue
channel.queue_declare(queue='dlx_q')
channel.queue_bind(queue='dlx_q', exchange='dlx')
channel.basic_consume(queue='dlx_q', auto_ack=False, on_message_callback=dlx_received)

print('Waiting for type2...')

try:
    while True:
        connection.process_data_events()
except KeyboardInterrupt:
    print("Process terminated by user")
finally:
    connection.close()
