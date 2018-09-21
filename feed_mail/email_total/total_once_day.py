from base import Base


class DayOnceClickRate(Base):
    def __init__(self):
        super(DayOnceClickRate, self).__init__()
        self.qsl_body_user = {
            #            "query": {
            #                "match": {
            #                    "packId": "1004261"
            #                }
            #            },
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
                                "request_active": {
                                    "filter": {
                                        "term": {
                                            "request_active": 1
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
        self.qsl_body_users = {
            "size": 0,
            "aggs": {
                "click_per_day": {
                    "date_histogram": {
                        "field": "timeLine",
                        "time_zone": "+08:00",
                        "interval": "day"
                    },
                    "aggs": {
                        "request_active": {
                            "filter": {
                                "term": {
                                    "request_active": 1
                                }
                            },
                            "aggs": {
                                "exusers": {
                                    "cardinality": {
                                        "field": "client_id"
                                    }
                                },
                                "clusers": {
                                    "filter": {"term": {"click": 1}},
                                    "aggs": {
                                        "users": {
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
            #           "query": {
            #               "match": {
            #                   "packId": "1004261"
            #               }
            #           },
            "size": 0,
            "aggs": {
                "click_per_day": {
                    "date_histogram": {
                        "field": "timeLine",
                        "time_zone": "+08:00",
                        "interval": "day"
                    },
                    "aggs": {
                        "request_active": {
                            "filter": {
                                "term": {
                                    "request_active": 1
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
                                        "exusers": {
                                            "cardinality": {
                                                "field": "client_id"
                                            }
                                        },
                                        "clusers": {
                                            "filter": {"term": {"click": 1}},
                                            "aggs": {
                                                "users": {
                                                    "cardinality": {
                                                        "field": "client_id"
                                                    }
                                                }
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
        result_three = {}
        result = self.request_es(self.qsl_body)
        result_user = self.request_es(self.qsl_body_user)
        result_users = self.request_es(self.qsl_body_users)
        for result in result['aggregations']['click_per_day']['buckets']:
            timestamp = result['key_as_string']
            exposure = {key: 0 for key in ['ig_new', 'ig_hot', 'ir', 'tsg']}
            exusers = {key: 0 for key in ['ig_new', 'ig_hot', 'ir', 'tsg']}
            click = {key: 0 for key in ['ig_new', 'ig_hot', 'ir', 'tsg']}
            clusers = {key: 0 for key in ['ig_new', 'ig_hot', 'ir', 'tsg']}
            avg_click = {key: 0 for key in ['ig_new', 'ig_hot', 'ir', 'tsg']}
            rate = {key: 0 for key in ['ig_new', 'ig_hot', 'ir', 'tsg']}
            for hr in result['request_active']['source']['buckets']:
                key = hr['key']
                exposure[key] = hr['exposure']['value']
                exusers[key] = hr['exusers']['value']
                click[key] = int(hr['total_click']['value'])
                clusers[key] = int(hr['clusers']['users']['value'])
                if clusers[key] != 0:
                    avg_click[key] = round(float(click[key]) / float(clusers[key]), 1)
                else:
                    avg_click[key] = 0.0
                rate[key] = str(round(hr['percentage']['value'], 2) if hr.get('percentage') else 0.0) + '%'
            result_one[timestamp] = exposure, exusers, click, clusers, avg_click, rate
        for result in result_user['aggregations']['click_per_day']['buckets']:
            timestamp = result['key_as_string']
            user = {key: 0 for key in ['ig_new', 'ig_hot', 'ir', 'tsg']}
            for hr in result['source']['buckets']:
                key = hr['key']
                user[key] = hr['request_active']['click_once']['distinct_user']['value']
            result_two[timestamp] = user
        for result in result_users['aggregations']['click_per_day']['buckets']:
            user = {}
            timestamp = result['key_as_string']
            user['exposure'] = result['request_active']['exusers']['value']
            user['click'] = result['request_active']['clusers']['users']['value']
            result_three[timestamp] = user
        return result_one, result_two, result_three


if __name__ == '__main__':
    result_one, result_two, result_three = DayOnceClickRate().feed_handle()
    for key, value in result_one.items():
        if key != "2018-08-18T00:00:00.000+08:00":
            print value[0]
            print value[1]
            print value[2]
            print value[3]
            print value[4]
            print value[5]
            # print result_two[key]
            print result_three[key]
