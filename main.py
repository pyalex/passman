import json
import datetime
import time
import uuid
import hashlib

import tornado.ioloop
import tornado.web
import tornado.httpserver
import tornado.websocket
import tornado.gen

import pika
import asyncmongo
import logging

MONGO_HOST = '127.0.0.1'
MONGO_PORT = 27017
MONGO_NAME = 'passman'
SERVER_PORT = 8080


class PassManActionMixin(object):
    storage = None

    @property
    def actions(self):
        return dict(
            update=self.update,
            get_updates=self.get_updates
        )

    @tornado.gen.coroutine
    def update(self, message, user_id):
        for record_id, data in message.get('records', {}).iteritems():
            yield tornado.gen.Task(
                self.storage.update,
                dict(record_id=record_id, user_id=user_id),
                dict(
                    record_id=record_id,
                    user_id=user_id,
                    data=data,
                    updated_at=datetime.datetime.now()
                ),
                upsert=True
            )

    @tornado.gen.coroutine
    def get_updates(self, message, user_id):
        try:
            timestamp = int(message['timestamp'])
        except (KeyError, ValueError, TypeError):
            timestamp = int(time.time())

        dt = datetime.datetime.fromtimestamp(timestamp)
        rows = (yield tornado.gen.Task(
            self.storage.find,
            dict(
                user_id=user_id,
                updated_at={'$gte': dt}
            )
        )).args[0]

        records = {}
        for row in rows:
            records[row['record_id']] = row['data']

        raise tornado.gen.Return(dict(upd_time=timestamp, records=records, sign=self.generate_sign(records)))


class PassManWebSocket(PassManActionMixin, tornado.websocket.WebSocketHandler):
    user_id = None
    sign_key = ''

    def __init__(self, application, request, **kwargs):
        tornado.websocket.WebSocketHandler.__init__(self, application, request, **kwargs)

        self.mongo_conn = asyncmongo.Client(pool_id='storage', host=MONGO_HOST, port=MONGO_PORT)
        self.app_id = request.arguments.get('app_id')[0] if request.arguments.get('app_id') else None


    @tornado.gen.coroutine
    def open(self):
        self.storage = self.mongo_conn.connection(collectionname='records', dbname=MONGO_NAME)
        credentials = self.mongo_conn.connection(collectionname='credentials', dbname=MONGO_NAME)

        user = (yield tornado.gen.Task(
            credentials.find_one,
            dict(application_id=self.app_id)
        )).args[0]

        if not user:
            self.write_error('Try authorize before')
            self.close()

        self.user_id = user['user_id']
        self.sign_key = user['sign_key']

    def generate_sign(self, records):
        return hashlib.sha1(
            str(hashlib.md5(json.dumps(records or {})).hexdigest()) +
            str(hashlib.md5(self.sign_key or '').hexdigest())
        ).hexdigest()

    def check_signature(self, message):
        return message.get('sign') == self.generate_sign(message.get('records'))

    @tornado.gen.coroutine
    def on_message(self, message):
        try:
            message = json.loads(message)
        except (TypeError, ValueError):
            self.write_error('Invalid message format')
            return

        if not self.check_signature(message):
            self.write_error('Invalid sign')
            return

        action = message.get('action')
        if not action or action not in self.actions:
            self.write_error('Invalid action')
            return

        response = (yield self.actions[action](message, self.user_id)) or {}
        response.update(status='success')
        self.write_message(response)

    def write_error(self, error):
        self.write_message(dict(message=error, status='error'))

    def on_close(self):
        print 'close'


class TokenHandler(tornado.web.RequestHandler):
    SUPPORTED_METHODS = ('POST', )

    @tornado.web.asynchronous
    @tornado.gen.coroutine
    def post(self, *args, **kwargs):
        mongo_conn = asyncmongo.Client(pool_id='credentials', host=MONGO_HOST, port=MONGO_PORT, dbname=MONGO_NAME)
        app_id = self.get_argument('app_id')
        sign_key = str(uuid.uuid4())

        user = (yield tornado.gen.Task(
            mongo_conn.users.find_one,
            dict(
                application_id=self.get_argument('app_id'),
                email=self.get_argument('email')
            )
        )).args[0]

        if not user:
            self.set_status(401)
            self.finish()
            return

        yield tornado.gen.Task(
            mongo_conn.credentials.update,
            dict(application_id=app_id),
            dict(
                application_id=app_id,
                user_id=user['_id'],
                sign_key=sign_key,
                created_at=datetime.datetime.now()
            ),
            upsert=True
        )

        self.write(dict(sign_key=sign_key, status='success'))
        self.finish()


class RegisterHandler(tornado.web.RequestHandler):
    SUPPORTED_METHODS = ('POST', )

    @tornado.web.asynchronous
    @tornado.gen.coroutine
    def post(self, *args, **kwargs):
        mongo_conn = asyncmongo.Client(pool_id='credentials', host=MONGO_HOST, port=MONGO_PORT, dbname=MONGO_NAME)

        result = yield tornado.gen.Task(
            mongo_conn.users.insert,
            dict(
                application_id=self.get_argument('app_id'),
                email=self.get_argument('email'),
                password=self.get_argument('hashed_pswd')
            ),
            safe=True
        )

        if result.kwargs.get('error'):
            self.send_error(status_code=400)
            return

        self.set_status(200)
        self.finish()

if __name__ == '__main__':
    application = tornado.web.Application([
        (r"/websocket", PassManWebSocket),
        (r"/register", RegisterHandler),
        (r"/get_connection", TokenHandler),
    ])
    server = tornado.httpserver.HTTPServer(application)
    server.listen(SERVER_PORT)

    logger = logging.getLogger('tornado.general')
    logger.addHandler(logging.StreamHandler())
    logger.setLevel(logging.DEBUG)

    logger_app = logging.getLogger('tornado.application')
    logger.addHandler(logging.StreamHandler())
    logger.setLevel(logging.DEBUG)

    tornado.ioloop.IOLoop.instance().start()