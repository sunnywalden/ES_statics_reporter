# -*- coding:utf-8 -*-
import pymongo
import redis
from elasticsearch import Elasticsearch


class Client:
    def __init__(self):
        self.password = 'k2u>aruFavT1lw[az'
        self.redis_host = 'redis_host'
        self.redis_port = redis_port
        self.redis_base = redis_db
        self.ES_HOSTS = ['http://host1:port', 'http://host2:port']
        self.es_conn = Elasticsearch(self.ES_HOSTS)
        self.pool = redis.ConnectionPool(host=self.redis_host,
                                         port=self.redis_port,
                                         db=self.redis_base,
                                         password=self.password)
        self.redis_conn = redis.Redis(connection_pool=self.pool)

    def get_mongo_client(self, hosts, replicaset):
        return pymongo.MongoClient(hosts,
                                   replicaset=replicaset,
                                   readPreference='secondaryPreferred',
                                   connect=False)[mongo_db][mongo_collection]

    def get_es_client(self):
        return self.es_conn

    def get_redis_client(self):
        return self.redis_conn
