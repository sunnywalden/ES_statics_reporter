from base import BaseKeywords
import uuid
import codecs

class HandleClickRate(BaseKeywords):
    def __init__(self):
        super(HandleClickRate, self).__init__()
        self.index = 'rate-keywords-feed-day'
	self.cpids = {}
        self.qsl = {
            "size": 0,
            "aggs": {
                "click_per_day": {
                    "date_histogram": {
                        "field": "timeLine",
                        "time_zone": "+08:00",
                        "interval": "day"
                },
                "aggs":{
                        "all_keywords":{
                            "terms":{
                               "field":"keywords.keyword",
                               "size": 100
                            },
                            "aggs": {
                                "total_videos": {
                                     "cardinality": {
                                         "field": "search_id"
                                     }
                                },
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
        #self.cpid_get()
        #print(self.cpids)
        result = self.client.es_conn.search(index=self.feed_index, doc_type=self.type, body=self.qsl, request_timeout=300)
        for result in result['aggregations']['click_per_day']['buckets']:
            for cl in result['all_keywords']['buckets']:
                #for hr in cl['all_cpid']['buckets']:
                    data = {}
                    data['timestamp'] = result['key_as_string']
                    data['timeLine'] = result['key_as_string'].split('.')[0] + '.000Z'
                    data['keywords'] = cl['key']
                    data['percentage'] = round(cl['percentage']['value'], 4) if cl.get('percentage') else 0.0
                    data['total_click'] = cl['total_click']['value']
                    data['total_videos'] = cl['total_videos']['value']
                    data['exposure'] = cl['exposure']['value']
                    #data['CP_ID'] = hr['key']
                    #cpid = hr['key'].encode('UTF-8')
                    #data['cp_name'] = self.cpids[cpid]
                    qsl_body.append({"index": {"_id": uuid.uuid1()}})
                    qsl_body.append(data)
            if qsl_body:
                self.request_es(self.index, qsl_body)


if __name__ == '__main__':
    HandleClickRate().run()
