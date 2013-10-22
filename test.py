import unittest
import pymongo
import json

import hashlib

import tornado.testing
import tornado.websocket
import tornado.web


import main

TEST_DB = 'test'
mongo_client = pymongo.Connection()
mongo_db = mongo_client[TEST_DB]

main.MONGO_NAME = 'test'

class PassmanWebSocketTest(tornado.testing.AsyncHTTPTestCase):
    def setUp(self):
        super(PassmanWebSocketTest, self).setUp()

        self.key = '111'
        mongo_db.credentials.insert({
            'application_id': '111',
            'user_id': 333,
            'sign_key': self.key
        })

    def get_app(self):
        return tornado.web.Application([
            (r"/websocket", main.PassManWebSocket),
        ])

    def get_protocol(self):
        return 'ws'

    def sign_message(self, message):
        message = message.copy()
        message['sign'] = hashlib.sha1(
            hashlib.md5(json.dumps(message.get('record') or {})).hexdigest() +
            hashlib.md5(self.key).hexdigest()
        ).hexdigest()
        return message

    @tornado.testing.gen_test
    def test_sign_check(self):
        client = yield tornado.websocket.websocket_connect(self.get_url('/websocket?app_id=111'), self.io_loop)

        client.write_message(json.dumps({'action': 'get_updates'}))
        resp = yield client.read_message()
        answer = json.loads(resp)

        self.assertEqual(answer['status'], 'error')
        self.assertEqual(answer['message'], 'Invalid sign')


    @tornado.testing.gen_test
    def test_successful_sign_check(self):
        client = yield tornado.websocket.websocket_connect(self.get_url('/websocket?app_id=111'), self.io_loop)

        client.write_message(json.dumps(self.sign_message({'action': 'get_updates'})))
        resp = yield client.read_message()
        answer = json.loads(resp)

        self.assertEqual(answer['status'], 'error')
        self.assertEqual(answer['message'], 'Invalid sign')


    def tearDown(self):
        super(PassmanWebSocketTest, self).tearDown()

        mongo_db.credentials.remove()


if __name__ == '__main__':
    unittest.main()