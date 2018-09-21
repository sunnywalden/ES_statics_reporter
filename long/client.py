# -*- coding:utf-8 -*-
import pymongo
import redis
from elasticsearch import Elasticsearch


class Client:
    def __init__(self):
        self.password = 'k2u>aruFavT1lw[az'
        self.redis_host = '10.200.26.120'
        self.redis_port = 6380
        self.redis_base = 13
        self.ES_HOSTS = ['http://10.150.30.94:9202', 'http://10.150.30.92:9202', 'http://10.150.30.91:9202',
                         'http://10.150.30.93:9202', 'http://10.150.30.88:9202', 'http://10.150.30.87:9202']
        self.es_conn = Elasticsearch(self.ES_HOSTS)
        self.pool = redis.ConnectionPool(host=self.redis_host,
                                         port=self.redis_port,
                                         db=self.redis_base,
                                         password=self.password)
        self.redis_conn = redis.Redis(connection_pool=self.pool)

    def get_es_client(self):
        return self.es_conn

    def get_redis_client(self):
        return self.redis_conn

    def get_mongo_client(self, host, replicaset):
        return pymongo.MongoClient(host,
                                   replicaset=replicaset,
                                   readPreference='secondaryPreferred',
                                   connect=False).migu_video.program
