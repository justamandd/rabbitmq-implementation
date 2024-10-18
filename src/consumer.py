import pika
from callbacks.win_toast_rmq_callback import *
import os
from dotenv import load_dotenv

load_dotenv()

class RabbitmqConsumer:
  def __init__(self, callback):
    self.__host = os.getenv('HOST')
    self.__port = int(os.getenv('PORT'))
    self.__username = os.getenv('RMQ_USERNAME')
    self.__password = os.getenv('PASSWORD')
    self.__queue = os.getenv('QUEUE_NAME')
    self.__callback = callback
    self.__channel = self.__create_channel()

  def __create_channel(self):
    connection_parameters = pika.ConnectionParameters(
      host=self.__host,
      port=self.__port,
      credentials=pika.PlainCredentials(
        username=self.__username, 
        password=self.__password
      )
    )

    channel = pika.BlockingConnection(connection_parameters).channel()
    channel.queue_declare(
      queue=self.__queue,
      durable=True,
    )

    channel.basic_consume(
      queue=self.__queue,
      auto_ack=False,
      on_message_callback=self.__on_message,
    )

    return channel
  
  def __on_message(self, ch, method, properties, body):
    try:
      self.__callback(ch, method, properties, body)
      ch.basic_ack(delivery_tag=method.delivery_tag)

    except Exception as e:
      print(f'Error: {e}')
      ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)


  def start(self):
    print(f'Listening to queue: custom_queue, on port 5672')
    self.__channel.start_consuming()

rabbitmq_consumer = RabbitmqConsumer(win_toast_rmq_callback)
rabbitmq_consumer.start()