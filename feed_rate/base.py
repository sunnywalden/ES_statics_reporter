from client import Client


class Base(object):
    def __init__(self):
        self.feed_index = 'feed_v1-*'
        self.type = 'logs'
        self.client = Client()
        self.body = {
            "settings": {
                "index.number_of_shards": 8,
                "number_of_replicas": 1
            },
            "mappings": {
                "logs": {
                    "properties": {
                        "timestamp": {
                            "type": "date"
                        },
                        "timeLine": {
                            "type": "date"
                        },
                        "percentage": {
                            "type": "float"
                        },
                        "total_click": {
                            "type": "integer"
                        },
                        "exposure": {
                            "type": "integer"
                        },
                        "source": {
                            "type": "keyword"
                        }
                    }
                }
            }
        }

    def request_es(self, rate_index, query_data):
        if self.client.es_conn.indices.exists(index=rate_index):
            self.client.es_conn.indices.delete(rate_index)
        self.client.es_conn.indices.create(rate_index, body=self.body)
        self.client.es_conn.bulk(index=rate_index, doc_type=self.type, body=query_data)
