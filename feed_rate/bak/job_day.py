from client import Client
import uuid


class HandleClickRate():
    def __init__(self):
        self.index = 'rate-feed-day'
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

    def main(self):
        if self.client.es_conn.indices.exists(index=self.index):
            self.client.es_conn.indices.delete(self.index)
        self.client.es_conn.indices.create(self.index, body=self.body)
        self.client.es_conn.bulk(index=self.index, doc_type=self.type, body=self.search())

    def search(self):
        qsl = {
            "size": 0,
            "aggs": {
                "click_per_day": {
                    "date_histogram": {
                        "field": "timeLine",
                        "time_zone": "+08:00",
                        "interval": "day"
                    },
                    "aggs": {
                        "source": {
                            "terms": {
                                "field": "source"
                            },
                            "aggs": {
                                "total_click": {
                                    "sum": {
                                        "field": "click"
                                    }
                                },
                                "exposure": {
                                    "value_count": {
                                        "field": "search_id"
                                    }
                                },
                                "percentage": {
                                    "bucket_script": {
                                        "buckets_path": {
                                            "exposure": "exposure",
                                            "totalClick": "total_click"
                                        },
                                        "script": "params.totalClick / params.exposure *100"
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        result = self.client.es_conn.search(index='feed_v1-*', doc_type=self.type, body=qsl)
        qsl_body = []
        for result in result['aggregations']['click_per_day']['buckets']:
            for hr in result['source']['buckets']:
                data = {}
                data['timestamp'] = result['key_as_string']
                data['percentage'] = round(hr['percentage']['value'], 4) if hr.get('percentage') else 0.0
                data['total_click'] = hr['total_click']['value']
                data['exposure'] = hr['exposure']['value']
                data['source'] = hr['key']
                qsl_body.append({"index": {"_id": uuid.uuid1()}})
                qsl_body.append(data)
        return qsl_body


if __name__ == '__main__':
    HandleClickRate().main()
