# -*- coding:utf-8 -*-

import time, uuid
from elasticsearch import Elasticsearch


class click:
    def __init__(self):
        self.timestamp = time.localtime(time.time() - 60 * 60)
        self.click_path = '/data/page_source/click/' + time.strftime('%Y/%m/%d/%H.txt', self.timestamp)
        self.ES_HOSTS = ['http://10.150.30.94:9202', 'http://10.150.30.92:9202', 'http://10.150.30.91:9202',
                         'http://10.150.30.93:9202', 'http://10.150.30.88:9202', 'http://10.150.30.87:9202']
        self.es_conn = Elasticsearch(self.ES_HOSTS)
        self.index = 'profile-' + time.strftime('%Y%m%d', time.localtime(time.time()))
        self.type = 'logs'
        self.mapping_body = {
            "settings": {
                "index.number_of_shards": 8,
                "number_of_replicas": 1
            },
            "mappings": {
                "logs": {
                    "properties": {
                        "page_id": {
                            "type": "keyword"
                        },
                        "click": {
                            "type": "integer"
                        },
                        "time": {
                            "type": "date",
                            "format": "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis"
                        },
                        "timestamp": {
                            "type": "date",
                            "format": "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis"
                        },
                        "user_id": {
                            "type": "keyword"
                        },
                        "search_id": {
                            "type": "keyword"
                        },
                        "client_id": {
                            "type": "keyword"
                        },
                        "watch_time": {
                            "type": "integer"
                        }
                    }
                }
            }
        }

    def handle_str_time(self, time_str):
        tim = time.mktime(time.strptime(time_str, '%Y-%m-%d %H:%M:%S'))
        front_time = time.localtime(tim - 8 * 3600)
        new_time = time.strftime("%Y-%m-%d %H:%M:%S", front_time)
        return new_time

    def every_hour_click_data(self):
        qsl_body = []
        with open(self.click_path, 'r') as fs:
            for f in fs:
                click_dict = {}
                line = f.strip().split('|')
                click_dict['page_id'] = line[0]
                click_dict['click'] = 1 if line[1] == 'click' else 0
                click_dict['time'] = line[2]

                click_dict['timestamp'] = self.handle_str_time(line[2])
                click_dict['user_id'] = line[3]
                click_dict['search_id'] = line[4]
                click_dict['client_id'] = line[5]
                click_dict['watch_time'] = line[6]
                qsl_body.append({"index": {"_id": str(uuid.uuid1())}})
                qsl_body.append(click_dict)
                if len(qsl_body) == 1000:
                    self.request_ES(qsl_body)
                    qsl_body = []
            self.request_ES(qsl_body)

    def request_ES(self, qsl_body):
        self.es_conn.bulk(index=self.index, doc_type=self.type, body=qsl_body)

    def create_index(self):
        if not self.es_conn.indices.exists(index=self.index):
            self.es_conn.indices.create(self.index, body=self.mapping_body)


if __name__ == '__main__':
    print 'start handle ......'
    click = click()
    click.create_index()
    click.every_hour_click_data()
