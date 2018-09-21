from base import Base
import uuid
import codecs

class HandleClickRate(Base):
    def __init__(self):
        super(HandleClickRate, self).__init__()
        self.index = 'rate-cpid-zeropack-feed-day'
	self.cpids = {}
        self.qsl = {
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
                "aggs":{
                        "channels":{
                            "terms":{
                               "field":"channel"
                            },
                    "aggs": {
                        "all_cpid": {
                            "terms": {
                                "field": "CP_ID"
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

    def cpid_get(self):
        cpids_dict = {}
        #with open('./cp_manager.csv','r') as f:
        with codecs.open(r'/opt/handle_log/feed_rate/cp_manager.csv', 'r', encoding='utf-8') as f:
            for line in f:
                cpid_dict = line.strip().split(',')
                print(cpid_dict)
                cpid = cpid_dict[0]
                if len(cpid_dict[1]) > 1:
                    
                    cpid_name = cpid_dict[1]
                     
                else:
                    cpid_name = cpid
                cpids_dict[cpid] = cpid_name
                print(cpid,cpid_name)
        self.cpids = cpids_dict
        #print(cpids_dict)

    def run(self):
        qsl_body = []
        self.cpid_get()
        #print(self.cpids)
        result = self.client.es_conn.search(index=self.feed_index, doc_type=self.type, body=self.qsl)
        for result in result['aggregations']['click_per_day']['buckets']:
            for cl in result['channels']['buckets']:
                for hr in cl['all_cpid']['buckets']:
                    data = {}
                    data['timestamp'] = result['key_as_string']
                    data['timeLine'] = result['key_as_string'].split('.')[0] + '.000Z'
                    data['channel'] = cl['key']
                    data['percentage'] = round(hr['percentage']['value'], 4) if hr.get('percentage') else 0.0
                    data['total_click'] = hr['total_click']['value']
                    data['exposure'] = hr['exposure']['value']
                    data['CP_ID'] = hr['key']
                    cpid = hr['key'].encode('UTF-8')
                    data['cp_name'] = self.cpids[cpid]
                    qsl_body.append({"index": {"_id": uuid.uuid1()}})
                    qsl_body.append(data)
            if qsl_body:
                self.request_es(self.index, qsl_body)


if __name__ == '__main__':
    HandleClickRate().run()
