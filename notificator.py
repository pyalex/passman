import json
import logging
from collections import defaultdict

import pika
import pika.adapters

import settings


logger_app = logging.getLogger('tornado.application')


class QueueClient(object):
    EXCHANGE_NAME = 'passman_exchange'

    def __init__(self, io_loop=None):
        logger_app.info('PikaClient: __init__')
        self.io_loop = io_loop
 
        self.connected = False
        self.connecting = False
        self.connection = None
        self.channel = None
        self.queue = None

        self.event_listeners = defaultdict(list)
 
    def connect(self):
        if self.connecting:
            logger_app.info('PikaClient: Already connected to RabbitMQ')
            return
 
        logger_app.info('PikaClient: Connecting to RabbitMQ')
        self.connecting = True
 
        cred = pika.PlainCredentials('guest', 'guest')
        param = pika.ConnectionParameters(
            host=settings.AMQP_HOST,
            port=settings.AMQP_PORT,
            virtual_host='/',
            credentials=cred
        )
 
        self.connection = pika.adapters.TornadoConnection(param, on_open_callback=self.on_connected)
        self.connection.add_on_close_callback(self.on_closed)
 
    def on_connected(self, connection):
        logger_app.info('PikaClient: connected to RabbitMQ')
        self.connected = True
        self.connection = connection
        self.connection.channel(self.on_channel_open)
 
    def on_channel_open(self, channel):
        logger_app.info('PikaClient: Channel open, Declaring exchange')
        self.channel = channel
        channel.exchange_declare(exchange=self.EXCHANGE_NAME, type='topic')
        channel.queue_declare(exclusive=True, auto_delete=True, callback=self.on_queue_declared)

    def on_queue_declared(self, method_frame):
        self.queue = method_frame.method.queue
        self.channel.basic_consume(self.on_message, self.queue)

    def on_closed(self, connection):
        logger_app.info('PikaClient: rabbit connection closed')
        self.io_loop.stop()
 
    def on_message(self, channel, method, header, body):
        logger_app.info('PikaClient: message received: %s' % body)

        event = json.loads(body)
        key = event.pop('key')

        for listener in self.event_listeners.get(key, []):
            listener.on_notify(event)
            logger_app.info('PikaClient: notified %s' % repr(listener))

    def get_routing_key(self, key):
        return 'users.' + key

    def send_notification(self, key, message):
        if not self.channel:
            raise RuntimeError

        message['key'] = key

        self.channel.basic_publish(self.EXCHANGE_NAME, routing_key=self.get_routing_key(key),
                                   body=json.dumps(message))

        logger_app.info('PikaClient: sending message %s' % repr(message))

    def add_event_listener(self, listener, key):
        if not self.channel:
            raise RuntimeError

        self.channel.queue_bind(exchange=self.EXCHANGE_NAME, queue=self.queue,
                                routing_key=self.get_routing_key(key), callback=None)
        self.event_listeners[key].append(listener)

        logger_app.info('PikaClient: listener %s added' % repr(listener))
 
    def remove_event_listener(self, listener, key):
        try:
            self.event_listeners[key].remove(listener)
        except (KeyError, ValueError):
            pass
