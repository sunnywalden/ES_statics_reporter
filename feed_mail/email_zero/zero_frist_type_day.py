from base import Base
import json


class DayFristTypeClickRate(Base):
    def __init__(self):
        super(DayFristTypeClickRate, self).__init__()
        self.qsl_body_user = {
            "query": {
                "match": {
                    "packId": "1002601"
                }
            },
            "size": 0,
            "aggs": {
                "click_per_day": {
                    "date_histogram": {
                        "field": "timeLine",
                        "time_zone": "+08:00",
                        "interval": "day"
                    },
                    "aggs": {
                        "frist_type": {
                            "terms": {
                                "field": "contDisplayName",
                                "size": 50
                            },
                            "aggs": {
                                "click_once": {
                                    "filter": {
                                        "term": {
                                            "click": 1
                                        }
                                    },
                                    "aggs": {
                                        "distinct_user": {
                                            "cardinality": {
                                                "field": "client_id"
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        self.qsl_body = {
            "query": {
                "match": {
                    "packId": "1002601"
                }
            },
            "size": 0,
            "aggs": {
                "click_per_day": {
                    "date_histogram": {
                        "field": "timeLine",
                        "time_zone": "+08:00",
                        "interval": "day"
                    },
                    "aggs": {
                        "frist_type": {
                            "terms": {
                                "field": "contDisplayName",
                                "size": 50
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

    def feed_handle(self):
        result_one = {}
        result_two = {}
        result = self.request_es(self.qsl_body)
        result_user = self.request_es(self.qsl_body_user)
        for result in result['aggregations']['click_per_day']['buckets']:
            timestamp = result['key_as_string']
            exposure, click, rate = {}, {}, {}
            for hr in result['frist_type']['buckets']:
                key = hr['key'].encode("utf-8")
                exposure[key] = hr['exposure']['value']
                click[key] = int(hr['total_click']['value'])
                rate[key] = str(round(hr['percentage']['value'], 2) if hr.get('percentage') else 0.0) + '%'
            result_one[timestamp] = json.dumps(exposure), json.dumps(click), json.dumps(rate)
            result_one[timestamp] = exposure, click, rate
        for result in result_user['aggregations']['click_per_day']['buckets']:
            timestamp = result['key_as_string']
            user = {}
            for hr in result['frist_type']['buckets']:
                user[hr['key'].encode("utf-8")] = hr['click_once']['distinct_user']['value']
            result_two[timestamp] = user
        return result_one, result_two


if __name__ == '__main__':
    result_one, result_two = DayFristTypeClickRate().feed_handle()
    for key, value in result_one.items():
        if key != "2018-08-18T00:00:00.000+08:00":
            print value[0]
            print value[1]
            print value[2]
            print result_two[key]
