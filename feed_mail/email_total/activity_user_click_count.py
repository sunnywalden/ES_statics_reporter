from base import Base
from datetime import date, timedelta

class ActivityUserClickCount(Base):
    def __init__(self):
        super(ActivityUserClickCount, self).__init__()
        self.yesterday = (date.today() - timedelta(1)).strftime("%Y-%m-%d") + 'T00:00:00.000+08:00'
        self.qsl_body_user = {
            "query": {
                "query_string": {
                    "query": "request_active:1 AND click:1"
                }
            },
            "size": 0,
            "aggs": {
                "users_per_day": {
                    "date_histogram": {
                        "field": "timeLine",
                        "time_zone": "+08:00",
                        "interval": "day"

                },
                "aggs": {
                    "active_users": {
                        "cardinality": {
                                "field": "client_id"
                        }
                    }

                }
                }
            }

        }

        self.qsl_body = {
            "query": {
                "query_string": {
                    "query": "request_active:1 AND click:1"
                }
            },
            "size": 0,
            "aggs": {
                "clicks_per_day": {
                    "date_histogram": {
                        "field": "timeLine",
                        "time_zone": "+08:00",
                        "interval": "day"

                },
                "aggs": {
                    "total_click": {
                        "sum": {
                            "field": "click"
                        }
                    }

                }
                }
            }
        }




    def feed_handle(self):
        results = {}
        result_one = {}
        result_two = {}
        result_list = []
        res = self.request_es(self.qsl_body)
        res_user = self.request_es(self.qsl_body_user)
        for result in res["aggregations"]["clicks_per_day"]["buckets"]:
            timestamp = result["key_as_string"]
            #total_click = {key: 0 for key in ["ig_new", "ig_hot", "ir", "tsg"]}
            #active_users = {key: 0 for key in ["ig_new", "ig_hot", "ir", "tsg"]}
            #avg_req_times = {key: 0 for key in ["ig_new", "ig_hot", "ir", "tsg"]}
            #hr = result["source"]["buckets"]
            click = result["total_click"]["value"]
#            users = int(result["active_users"]["value"])
#            times = str(round(result["avg_req_times"]["value"], 2) if result.get("avg_req_times") else 0.0) + "%"
            result_one[timestamp] = click
        for result in res_user["aggregations"]["users_per_day"]["buckets"]:
            timestamp = result["key_as_string"]
#            user = {key: 0 for key in ["ig_new", "ig_hot", "ir", "tsg"]}
#            for hr in result["source"]["buckets"]:
            user = result["active_users"]["value"]
            result_two[timestamp] = user
        for key, value in result_one.items():
            time_stamp = key
            click_num = value
            user_num = result_two[key]
            avg_click = round(click_num / user_num, 2)
            results[time_stamp] = [click_num, user_num, avg_click]
            result_list.append([click_num, user_num, avg_click])
#        return result_one, result_two
        #print(result_list)
        #n = len(result_list)
        #i = n - 1 
        #return result_list[i][0],result_list[i][1],result_list[i][2]
        print(self.yesterday)
        return results[self.yesterday][0],results[self.yesterday][1],results[self.yesterday][2]
       
        #return results

if __name__ == "__main__":
    click_num,user_num,avg_click = ActivityUserClickCount().feed_handle()
    print(click_num,user_num,avg_click)
#    for key, value in result.items():
#        if key != "2018-08-18T00:00:00.000+08:00":
            #print key
#            print value[0]
#            print value[1]
#            print value[2]
            #print result_two[key]
