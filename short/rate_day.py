from base import Base
import uuid


class HandleClickRate(Base):
    def __init__(self):
        super(HandleClickRate, self).__init__()
        self.index = 'rate-short-day'
        self.qsl = {
          "size":0,
          "aggs":{
          "click_per_day":{
            "date_histogram":{
                "field":"timeLine",
                "time_zone":"+08:00",
                "interval":"day"
            },
            "aggs":{
                "sources":{
                    "terms":{
                        "field":"source"
                    },
                            "aggs":{
                                "total_click":{
                                    "sum":{
                                        "field":"click"
                                    }
                                },
                                "exposure":{
                                    "value_count":{
                                        "field":"search_id"
                                    }
                                },
                                "exusers":{
                                    "cardinality":{
                                        "field":"client_id"
                                    }
                                },
                                "clusers":{
                                    "filter":{
                                        "term":{
                                            "click":1
                                        }
                                    },
                                    "aggs":{
                                        "users":{
                                            "cardinality":{
                                                "field":"client_id"
                                            }
                                        }
                                    }
                                },
                                "percentage":{
                                    "bucket_script":{
                                        "buckets_path":{
                                            "exposure":"exposure",
                                            "totalClick":"total_click"
                                        },
                                        "script":"params.totalClick / params.exposure *100"
                                    }
                                }
                            }
                        }
                    }
                }
            }
          }



        

    def run(self):
        qsl_body = []
        result = self.client.es_conn.search(index=self.feed_index, doc_type=self.type, body=self.qsl)
        if len(result['aggregations']['click_per_day']['buckets']) >= 1:
          for result in result['aggregations']['click_per_day']['buckets']:
            click_num = 0
            exposure_num = 0
            rate = 0.0
            total_data = {}

            if len(result['sources']['buckets']) >= 1:
                for hr in result['sources']['buckets']:
                    # click_num = 0
                    # exposure_num = 0
                    # rate = 0.0
                    # total_data = {}
                    all_data = {}
                    data = {}
                    all_data['timestamp'] = result['key_as_string']
                    all_data['timeLine'] = result['key_as_string'].split('.')[0] + '.000Z'
                    all_data['source'] = hr['key']
                #data['timestamp'] = result['key_as_string']
                # data['timeLine'] = result['key_as_string'].split('.')[0].replace('T', ' ')
                #data['timeLine'] = result['key_as_string'].split('.')[0] + '.000Z'
                    all_data['percentage'] = round(hr['percentage']['value'], 4) if hr.get('percentage') else 0.0
                    all_data['total_click'] = hr['total_click']['value']
                    all_data['exposure'] = hr['exposure']['value']
                    click_num += all_data['total_click']
                    exposure_num += all_data['exposure']
                    all_data['source'] = hr['key']
                    qsl_body.append({"index": {"_id": uuid.uuid1()}})
                    qsl_body.append(all_data)
                    rate += round(float(click_num / exposure_num) * 100, 4)
                    print(click_num,exposure_num,rate)
            total_data['timestamp'] = result['key_as_string']
            total_data['timeLine'] = result['key_as_string'].split('.')[0] + '.000Z'
            total_data['total_click'] = click_num
            total_data['exposure'] = exposure_num
            rate = round(float(click_num / exposure_num) * 100, 4)
            total_data['percentage'] = rate
            total_data['source'] = 'all'
            print(click_num, exposure_num, rate)
            qsl_body.append({"index": {"_id": uuid.uuid1()}})
            qsl_body.append(total_data)
            if qsl_body:
                self.request_es(self.index, qsl_body)


if __name__ == '__main__':
    HandleClickRate().run()
