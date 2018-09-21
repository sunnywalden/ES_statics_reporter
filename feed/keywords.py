# -*- coding: utf-8 -*-
from base import Base
import uuid
import chardet
import re
import time, datetime


class HandleKeywordStatics(Base):
    def __init__(self):
        super(HandleKeywordStatics, self).__init__()
        self.index = 'keywords-day'

    def run(self, keyword, starttime, endtime):
        qsl_body = []

        encode_name = chardet.detect(keyword)['encoding']

        words = []
        for c in keyword.decode(encode_name):
            words.append(c)
        final_words = '(' + ' AND '.join(words) + ')'

        time_now = datetime.datetime.today()
        pattern = re.compile(r'\d+')
        if re.search('now', starttime):
            n = pattern.findall(starttime)
            start_time = (time_now + datetime.timedelta(days=n)).strftime("%Y%m%d") + ' 00:00:00'
        if re.search('now', endtime):
            m = pattern.findall(endtime)
            end_time = (time_now + datetime.timedelta(days=m)).strftime("%Y%m%d") + ' 00:00:00'
        #now = time.strftime("%Y-%m-%d", time_now)

        if re.search(r"\d[8]", starttime):
            start_time = starttime[0:4] + '-' + starttime[4:6] + '-' + starttime[6:8] + ' 00:00:00'
        if re.search(r"\d[8]", endtime):
            end_time = endtime[0:4] + '-' + endtime[4:6] + '-' + endtime[6:8] + ' 00:00:00'
        if re.search('now', starttime):
            start_time = time_now

        if endtime == 'now':
            end_time = now
        qsl = {

            "query": {
                "bool": {
                    "must": {
                        "match": {
                            "keywords": final_words
                        }
                    },
                    "filter": {
                        "range": {
                            "timeStamp": {
                                "gte": start_time,
                                "lt": end_time,
                                "format": "yyyy-MM-dd HH:mm:ss",
                                "time_zone": "+08:00"
                            }
                        }
                    }
                }
            },
            "size": 0,
            "aggs": {
                "exusers": {
                    "cardinality": {
                        "field": "client_id"
                    }
                },
                "clusers": {
                    "filter": {
                        "term": {
                            "click": 1
                        }
                    },
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
        result = self.client.es_conn.search(index=self.feed_index, doc_type=self.type, body=qsl)
        exposure = result['aggregations']['exusers']['value']
        click = result['aggregations']['clusers']['users']['value']
        print('曝光量', exposure, '点击量', click)


if __name__ == '__main__':
    HandleKeywordStatics().run('西班牙', '20180912', '20180913')
