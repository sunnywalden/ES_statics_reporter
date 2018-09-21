# -*- coding:utf-8 -*-
import pymongo
import redis
from rediscluster import StrictRedisCluster
from elasticsearch import Elasticsearch


class Client:
    def __init__(self):
        self.password = 'k2u>aruFavT1lw[az'
        self.redis_host = '10.200.26.24'
        self.redis_port = 6380
        self.redis_base = 11
        self.redis_new_base = 12
        self.ES_HOSTS = ['http://10.150.30.94:9202', 'http://10.150.30.92:9202', 'http://10.150.30.91:9202',
                         'http://10.150.30.93:9202', 'http://10.150.30.88:9202', 'http://10.150.30.87:9202']
        self.es_conn = Elasticsearch(self.ES_HOSTS)
        self.pool = redis.ConnectionPool(host=self.redis_host,
                                         port=self.redis_port,
                                         db=self.redis_base,
                                         password=self.password)
        self.redis_conn = redis.Redis(connection_pool=self.pool)
        self.new_pool = redis.ConnectionPool(host=self.redis_host,
                                         port=self.redis_port,
                                         db=self.redis_new_base,
                                         password=self.password)
        self.new_redis_conn = redis.Redis(connection_pool=self.new_pool)
        self.cluster_password = "r789bihw!33%32jjn"
        self.nodes = [
            {"host": "10.200.26.25", "port": "6390"},
            {"host": "10.200.26.25", "port": "6391"},
            {"host": "10.200.26.25", "port": "6392"},
            {"host": "10.200.26.25", "port": "6393"},
            {"host": "10.200.26.25", "port": "6394"},
            {"host": "10.200.26.25", "port": "6395"}
        ]
        self.redis_cluster_conn = StrictRedisCluster(startup_nodes=self.nodes, password=self.cluster_password)

    def get_mongo_client(self, hosts, replicaset):
        return pymongo.MongoClient(hosts,
                                   replicaset=replicaset,
                                   readPreference='secondaryPreferred',
                                   connect=False).migu_video.program
    
    def get_profile_mongo_client(self, hosts, database, collection):
        return pymongo.MongoClient(hosts,
                                   readPreference='secondaryPreferred',
                                   connect=False)[database][collection]

    def get_es_client(self):
        return self.es_conn

    def get_redis_client(self):
        return self.redis_conn
    
    def get_redis_new_client(self):
        return self.new_redis_conn

    def get_redis_cluster_client(self):
        return self.redis_cluster_conn
