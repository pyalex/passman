import json
import time
import uuid
import hashlib
import logging
import datetime

import tornado.ioloop
import tornado.web
import tornado.httpserver
import tornado.websocket
import tornado.gen
import tornado.concurrent

import asyncmongo
from bson.objectid import ObjectId

from notificator import QueueClient
from utils import to_future

import settings

logger_app = logging.getLogger('tornado.application')


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
        dt = datetime.datetime.now()
        for record_id, data in message.get('records', []):
            yield tornado.gen.Task(
                self.storage.update,
                dict(record_id=record_id, user_id=ObjectId(user_id)),
                dict(
                    record_id=record_id,
                    user_id=ObjectId(user_id),
                    data=data,
                    updated_at=dt
                ),
                upsert=True
            )

        self.send_notification(dt)

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
                user_id=ObjectId(user_id),
                updated_at={'$gte': dt}
            )
        )).args[0]

        records = []
        for row in rows:
            records.append((row['record_id'], row['data']))

        raise tornado.gen.Return(dict(upd_time=timestamp, records=records, sign=self.generate_sign(records)))


class PassManWebSocket(PassManActionMixin, tornado.websocket.WebSocketHandler):
    user_id = None
    sign_key = ''

    def __init__(self, application, request, **kwargs):
        tornado.websocket.WebSocketHandler.__init__(self, application, request, **kwargs)

        self.mongo_conn = asyncmongo.Client(pool_id='storage', host=settings.MONGO_HOST, port=settings.MONGO_PORT)
        self.app_id = request.arguments.get('app_id')[0] if request.arguments.get('app_id') else None
        self.skip_notification = False


    @tornado.gen.coroutine
    def open(self):
        self.storage = self.mongo_conn.connection(collectionname='records', dbname=settings.MONGO_NAME)
        credentials = self.mongo_conn.connection(collectionname='credentials', dbname=settings.MONGO_NAME)

        user = (yield tornado.gen.Task(
            credentials.find_one,
            dict(application_id=self.app_id)
        )).args[0]

        if not user:
            self.write_error('Try authorize before')
            self.close()

        self.user_id = str(user['user_id'])
        self.sign_key = user['sign_key']

        self.application.queue.add_event_listener(self, self.user_id)

    def on_notify(self, message):
        if self.skip_notification:
            return
        
        self.write_message(message)
        self.skip_notification = False

    def send_notification(self, dt):
        self.skip_notification = True
        self.application.queue.send_notification(self.user_id, dict(
            new=int(time.mktime(dt.timetuple()))
        ))

    def generate_sign(self, records):
        return hashlib.sha1(
            str(hashlib.md5(json.dumps(records or [])).hexdigest()) +
            str(hashlib.md5(self.sign_key or '').hexdigest())
        ).hexdigest()

    def check_signature(self, message):
        if not message.get('records'):
            return True

        return message.get('sign') == self.generate_sign(message.get('records'))

    @tornado.gen.coroutine
    def on_message(self, message):
        logger_app.info(message)

        try:
            message = json.loads(message)
        except (TypeError, ValueError):
            self.write_error('Invalid message format')
            return
        logger_app.info(message)

        if not self.check_signature(message):
            self.write_error('Invalid sign')
            return
        logger_app.info('sign checked')

        action = message.get('action')
        if not action or action not in self.actions:
            self.write_error('Invalid action')
            return

        logger_app.info(action)

        response = (yield self.actions[action](message, self.user_id)) or {}
        response.update(status='success')
        self.write_message(response)

        logger_app.info(response)

    def write_error(self, error):
        self.write_message(dict(message=error, status='error'))

    def on_close(self):
        self.application.queue.remove_event_listener(self, self.user_id)



class ExceptionHandlerMixin(object):
    def _handle_request_exception(self, e):
        if self._finished:
            return

        try:
            raise e
        except asyncmongo.errors.IntegrityError:
            self.set_status(400)
        except asyncmongo.errors.Error:
            self.set_status(500)
        except:
            super(ExceptionHandlerMixin, self)._handle_request_exception(e)

        if not self._finished:
            self.write(dict(status='error', message=str(e)))
            self.finish()


class TokenHandler(ExceptionHandlerMixin, tornado.web.RequestHandler):
    SUPPORTED_METHODS = ('POST', )

    @tornado.web.asynchronous
    @tornado.gen.coroutine
    def post(self, *args, **kwargs):
        mongo_conn = asyncmongo.Client(pool_id='credentials', host=settings.MONGO_HOST, port=settings.MONGO_PORT, dbname=settings.MONGO_NAME)
        app_id = self.get_argument('app_id')
        sign_key = str(uuid.uuid4())

        user = yield to_future(mongo_conn.users.find_one)(
            dict(
                application_id=self.get_argument('app_id'),
                email=self.get_argument('email'),
                password=self.get_argument('hashed_pswd')
            )
        )

        if not user:
            self.set_status(401)
            self.write(dict(status='error', message='unauthorized'))
            self.finish()
            return

        yield to_future(mongo_conn.credentials.update)(
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


class RegisterHandler(ExceptionHandlerMixin, tornado.web.RequestHandler):
    SUPPORTED_METHODS = ('POST', )

    @tornado.web.asynchronous
    @tornado.gen.coroutine
    def post(self, *args, **kwargs):
        mongo_conn = asyncmongo.Client(pool_id='credentials', host=settings.MONGO_HOST, port=settings.MONGO_PORT, dbname=settings.MONGO_NAME)

        yield to_future(mongo_conn.users.insert)(
            dict(
                application_id=self.get_argument('app_id'),
                email=self.get_argument('email'),
                password=self.get_argument('hashed_pswd')
            ),
            safe=True
        )

        self.set_status(200)
        self.finish()


if __name__ == '__main__':
    application = tornado.web.Application([
        (r"/websocket", PassManWebSocket),
        (r"/register", RegisterHandler),
        (r"/get_connection", TokenHandler),
    ])
    server = tornado.httpserver.HTTPServer(application)
    server.listen(settings.SERVER_PORT)

    ioloop = tornado.ioloop.IOLoop.instance()
    application.queue = QueueClient(ioloop)
    application.queue.connect()

    logger = logging.getLogger('tornado.general')
    logger.setLevel(logging.DEBUG)

    logger_app.setLevel(logging.DEBUG)

    ioloop.start()