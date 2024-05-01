import time
import pika
from pika.exchange_type import ExchangeType

items = [{'key': 'type1',
          'info': 'Data for first consumer'},
         {'key': 'type2',
          'info': 'Data for second consumer'},
         {'key': 'type3',
          'info': 'Data for third consumer'},
         {'key': 'type2',
          'info': 'Second data for second consumer'}]

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()
channel.exchange_declare(exchange='exch_topic', exchange_type='topic')

def send_replay(channel, items):
    selected_category = items
    routing_key = f"items.{selected_category['key']}"
    message = selected_category['info']
    channel.basic_publish(exchange='exch_topic', routing_key=routing_key, body=message, properties=pika.BasicProperties(expiration='5000'))
    print(f"Element {selected_category['key']} with {message}")

try:
    for item in items:
        send_replay(channel, item)
        time.sleep(5)
except KeyboardInterrupt:
    print("Process terminated by user")
finally:
    connection.close()
