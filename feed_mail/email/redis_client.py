# -*- coding:utf-8 -*-
from rediscluster import StrictRedisCluster



class RedisClient:
    def __init__(self):
        self.password="r789bihw!33%32jjn"
        self.startup_nodes = [
            {"host": "10.200.26.25", "port": "6390"},
            {"host": "10.200.26.25", "port": "6391"},
            {"host": "10.200.26.25", "port": "6392"},
            {"host": "10.200.26.25", "port": "6393"},
            {"host": "10.200.26.25", "port": "6394"},
            {"host": "10.200.26.25", "port": "6395"}
        ]
        self.redis_conn = StrictRedisCluster(startup_nodes=self.startup_nodes, password=self.password)

    def get_redis_client(self):
        return self.redis_conn
if __name__ == '__main__':
    redis = RedisClient().get_redis_client()
    redis.set('gouhaiding_v1','gouhaiding_0',ex=60*60)
    redis.set('gouhaiding_v1','gouhaiding_1',ex=60*60)
    value = redis.get('gouhaiding_v1')
    print value

