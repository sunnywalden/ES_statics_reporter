from client import Client
import time


class Base(object):
    def __init__(self):
        self.timestamp = time.localtime(time.time() - 24 * 60 * 60)
        #self.timestamp = time.localtime(time.time() - 7 * 24 * 60 * 60)
        self.day = time.strftime('%Y%m%d', self.timestamp)
        self.feed_index = 'feed_v1-' + self.day
        self.type = 'logs'
        self.client = Client()

    def request_es(self, qsl_body):
        result = self.client.es_conn.search(index=self.feed_index, doc_type=self.type, body=qsl_body)
        return result
