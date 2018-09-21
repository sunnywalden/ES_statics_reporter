# -*- coding: utf-8 -*-
from base import BaseProfile
from constant import PAGE_ID, PAGE_ID_NAME
import json

class SubchannelCount(BaseProfile):
    def __init__(self):
        super(SubchannelCount, self).__init__()
        self.gen_dict = dict(zip(PAGE_ID, PAGE_ID_NAME))

        self.qsl_body = {
            "query": {
                "query_string": {
                    "query": "click:1"
                }
            },
            "size": 0,
            "aggs": {
                "clicks_per_day": {
                    "date_histogram": {
                        "field": "timestamp",
                        #"time_zone": "+08:00",
                        "interval": "day"

                },
                "aggs": {
                    "channels": {
                       "terms": {
                           "field": "page_id",
                            "size": 500
                        },
                        "aggs": {
                            "total_click": {
                                "sum": {
                                    "field": "click"
                                    }
                                },
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




    def feed_handle(self):
        results = {}
        final_result = []
        result_one = {}
        result_two = {}
        res = self.request_es(self.qsl_body)
        #for result in res["aggregations"]["clicks_per_day"]["buckets"]:
        result = res["aggregations"]["clicks_per_day"]["buckets"][1]
        #print(result)
        timestamp = result["key_as_string"]
        pageid_dict = self.gen_dict
            #for k,v in pageid_dict.items():
            #    print(k,json.dumps(v))
                
        channel_data = result["channels"]["buckets"]
        for hr in channel_data:
                channel = hr["key"] 
                channel_code = hr["key"].encode('UTF-8')
                print('debug info before todo',channel_code) 
                if pageid_dict.has_key(channel_code):
                    channel_name = pageid_dict[channel_code]
                    user = hr["distinct_user"]["value"]
                    click = int(hr["total_click"]["value"])
                    print(channel_code,channel_name,click,user)
                    #results[timestamp].append([channel_name,click,user])
                    final_result.append([channel_code,channel_name,click,user])
                else:
                    print(channel_code,'is not in dict')
                    pass
        #print(results)
        #return results
        #print(final_result)
        return final_result


if __name__ == "__main__":
    result = SubchannelCount().feed_handle()
    #for key, value in result.items():
    for value in result:
        #if key != "2018-08-18T00:00:00.000+08:00":
            #print key
            print value[0]
            print value[1]
            print value[2]
            print value[3]
