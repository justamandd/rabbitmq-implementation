from typing import Dict
import pika
import json

import os
from dotenv import load_dotenv

load_dotenv()

class RabbitmqPublisher:
  def __init__(self):
    self.__host = os.getenv('HOST')
    self.__port = int(os.getenv('PORT'))
    self.__username = os.getenv('RMQ_USERNAME')
    self.__password = os.getenv('PASSWORD')
    self.__exchange = os.getenv('EXCHANGE_NAME')
    self.__channel = self.__create_channel()
  
  def __create_channel(self):
    connection_parameters = pika.ConnectionParameters(
        host=self.__host,
        port=self.__port,
        credentials=pika.PlainCredentials(
          username=self.__username, 
          password=self.__password
        ),
    )

    channel = pika.BlockingConnection(connection_parameters).channel()

    return channel
  
  def publish(self, data: Dict):
    self.__channel.basic_publish(
        exchange=self.__exchange,
        routing_key='',
        body=json.dumps(data),
        properties=pika.BasicProperties(
          delivery_mode=2
        )
    )

rabbitmq_publisher = RabbitmqPublisher()

while True:
  data = {
    'title': input('Titulo da Mensagem: '),
    'message': input('Corpo da Mensagem: '),
  }

  rabbitmq_publisher.publish(data)