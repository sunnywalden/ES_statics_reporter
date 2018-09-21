# -*- coding:utf-8 -*-
from base import Base


class DaySourceClickRate(Base):
    def __init__(self):
        super(DaySourceClickRate, self).__init__()
        self.qsl_body_user = {
            "query": {
                "bool": {
                    "must": [
                        {
                            "match": {
                                "packId": "1002601"
                            }
                        },
                        {
                            "match": {
                                "video_source": "天脉"
                            }
                        }
                    ]
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
                        "source": {
                            "terms": {
                                "field": "source"
                            },
                            "aggs": {
                                "four_source": {
                                    "filter": {
                                        "term": {
                                            "four_source": 1
                                        }
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
            }
        }
        self.qsl_body = {
            "query": {
                "bool": {
                    "must": [
                        {
                            "match": {
                                "packId": "1002601"
                            }
                        },
                        {
                            "match": {
                                "video_source": "天脉"
                            }
                        }
                    ]
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
                        "four_source": {
                            "filter": {
                                "term": {
                                    "four_source": 1
                                }
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
            }
        }

    def feed_handle(self):
        result_one = {}
        result_two = {}
        result = self.request_es(self.qsl_body)
        result_user = self.request_es(self.qsl_body_user)
        for result in result['aggregations']['click_per_day']['buckets']:
            timestamp = result['key_as_string']
            exposure = {key: 0 for key in ['ig_new', 'ig_hot', 'ir', 'tsg']}
            click = {key: 0 for key in ['ig_new', 'ig_hot', 'ir', 'tsg']}
            rate = {key: 0 for key in ['ig_new', 'ig_hot', 'ir', 'tsg']}
            for hr in result['four_source']['source']['buckets']:
                key = hr['key']
                exposure[key] = hr['exposure']['value']
                click[key] = int(hr['total_click']['value'])
                rate[key] = str(round(hr['percentage']['value'], 2) if hr.get('percentage') else 0.0) + '%'
            result_one[timestamp] = exposure, click, rate
        for result in result_user['aggregations']['click_per_day']['buckets']:
            timestamp = result['key_as_string']
            user = {key: 0 for key in ['ig_new', 'ig_hot', 'ir', 'tsg']}
            for hr in result['source']['buckets']:
                key = hr['key']
                user[key] = hr['four_source']['click_once']['distinct_user']['value']
            result_two[timestamp] = user
        return result_one, result_two


if __name__ == '__main__':
    result_one, result_two = DaySourceClickRate().feed_handle()
    for key, value in result_one.items():
        if key != "2018-08-18T00:00:00.000+08:00":
            print value[0]
            print value[1]
            print value[2]
            print result_two[key]