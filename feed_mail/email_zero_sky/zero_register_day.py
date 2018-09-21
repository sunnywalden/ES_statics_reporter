# -*- coding:utf-8 -*-
from base import Base


class DayRegisterClickRate(Base):
    def __init__(self):
        super(DayRegisterClickRate, self).__init__()
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
                        "source": {
                            "terms": {
                                "field": "source"
                            },
                            "aggs": {
                                "register": {
                                    "terms": {
                                        "field": "register_user"
                                    },
                                    "aggs": {
                                        "click_once": {
                                            "filter": {
                                                "term": {
                                                    "click": 1
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

    def feed_handle(self):
        result_one = {}
        result_two = {}
        result = self.request_es(self.qsl_body)
        for result in result['aggregations']['click_per_day']['buckets']:
            timestamp = result['key_as_string']
            register = {key: 0 for key in ['ig_new', 'ig_hot', 'ir', 'tsg']}
            no_register = {key: 0 for key in ['ig_new', 'ig_hot', 'ir', 'tsg']}
            for hr in result['source']['buckets']:
                for h in hr['register']['buckets']:
                    if h['key'] == 1:
                        register[hr['key']] = h['click_once']['doc_count']
                    else:
                        no_register[hr['key']] = h['click_once']['doc_count']
            result_one[timestamp] = register
            result_two[timestamp] = no_register
        return result_one, result_two


if __name__ == '__main__':
    result_one, result_two = DayRegisterClickRate().feed_handle()
    for key, value in result_one.items():
        if key != "2018-08-18T00:00:00.000+08:00":
            print value
            print result_two[key]
