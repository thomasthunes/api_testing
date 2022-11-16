
import json
import logging
import time
import urllib
import uuid

import pika

from pika.adapters.tornado_connection import TornadoConnection


class PikaClient(object):

    def __init__(self, config: dict, exchanges: dict) -> None:
        self.connecting = False
        self.connection = None
        self.channel = None
        self.config = config
        self.exchanges = exchanges

    def connect(self):
        if self.connecting:
            logging.info('connecting - so not re-connecting')
            return
        self.connecting = True
        host = self.config.get('host')
        port = 5671 if self.config.get('amqps') else 5672
        scheme = 'amqps' if self.config.get('amqps') else 'amqp'
        virtual_host = urllib.parse.quote(self.config.get('vhost'), safe='')
        user = self.config.get('user')
        pw = self.config.get('pw')
        heartbeat = self.config.get('heartbeat') if self.config.get('heartbeat') else 0
        params = pika.URLParameters(
            f"{scheme}://{user}:{pw}@{host}:{port}/{virtual_host}?heartbeat={heartbeat}"
        )
        self.connection = TornadoConnection(params)
        self.connection.add_on_open_callback(self.on_connect)
        self.connection.add_on_close_callback(self.on_closed)
        self.connection.add_on_open_error_callback(self.on_open_error_callback)
        self.connecting = False
        return

    def on_open_error_callback(self, connection: TornadoConnection, exception: Exception) -> None:
        logging.error('could not connect')

    def on_connect(self, connection: TornadoConnection) -> None:
        self.connection = connection
        self.channel = self.connection.channel(
            on_open_callback=self.on_channel_open
        )
        return

    def on_channel_open(self, channel: pika.channel.Channel) -> None:
        for backend, config in self.exchanges.items():
            ex_name = config.get('exchange')
            channel.exchange_declare(
                ex_name,
                exchange_type='topic',
                durable=True,
            )
            logging.info(f'rabbitmq exchange: {ex_name} declared')
        return

    def on_basic_cancel(self, frame: pika.frame.Frame) -> None:
        self.connection.close()

    def on_closed(self, connection: TornadoConnection, exception: Exception) -> None:
        logging.info('rabbitmq connection closed')
        logging.info(exception)

    def publish_message(
        self,
        *,
        exchange: str,
        routing_key: str,
        method: str,
        uri: str,
        version: str,
        data: dict,
        persistent: bool = True,
        timestamp: int = int(time.time()),
    ) -> None:
        """
        Publilsh a message to an exchange.

        Parameters
        ----------
        exchange: str, exchange name
        routing_key: str, routing key for topic exchange
        method: str, HTTP method
        uri: str, HTTP request URI
        version: str, e.g. v1
        data: dict
        persistent: bool, default True
            tell rabbitmq to persist messages to disk, or not

        """
        data = {
            'method': method,
            'uri': uri,
            'version': version,
            'data': data,
        }
        message = json.dumps(data)
        delivery_mode = 2 if persistent else 1
        self.channel.basic_publish(
            exchange=exchange,
            routing_key=routing_key,
            body=message,
            properties=pika.BasicProperties(
                content_type='application/json',
                delivery_mode=delivery_mode,
                timestamp=timestamp,
                message_id=str(uuid.uuid4()),
            )
        )
        return
