from base import Base


class RegisterUserCount(Base):
    def __init__(self):
        super(RegisterUserCount, self).__init__()
        self.qsl_body_active_user_request = {
            "query": {
                "query_string": {
                    "query": "request_active:1"
                }
            },
            "size": 0,
            "aggs": {
                "request_per_day": {
                    "date_histogram": {
                        "field": "timeLine",
                        "time_zone": "+08:00",
                        "interval": "day"

                },
                "aggs": {
                    "active_requests": {
                        "cardinality": {
                                "field": "request"
                        }
                    }

                }
                }
            }

        }

        self.qsl_body_active_user = {
            "query": {
                "query_string": {
                    "query": "request_active:1"
                }
            },
            "size": 0,
            "aggs": {
                "active_user_per_day": {
                    "date_histogram": {
                        "field": "timeLine",
                        "time_zone": "+08:00",
                        "interval": "day"

                },
                "aggs": {
                    "total_active_user": {
                        "cardinality": {
                            "field": "client_id"
                        }
                    }

                }
                }
            }
        }


        self.qsl_body_register_user_request = {
            "query": {
                "query_string": {
                    "query": "register_user:1"
                }
            },
            "size": 0,
            "aggs": {
                "register_user_request_per_day": {
                    "date_histogram": {
                        "field": "timeLine",
                        "time_zone": "+08:00",
                        "interval": "day"

                },
                "aggs": {
                    "total_register_user_request": {
                        "cardinality": {
                            "field": "request"
                        }
                    }

                }
                }
            }
        }


        self.qsl_body_unregister_user_request = {
            "query": {
                "query_string": {
                    "query": "register_user:0"
                }
            },
            "size": 0,
            "aggs": {
                "unregister_user_request_per_day": {
                    "date_histogram": {
                        "field": "timeLine",
                        "time_zone": "+08:00",
                        "interval": "day"

                },
                "aggs": {
                    "total_unregister_user_request": {
                        "cardinality": {
                            "field": "request"
                        }
                    }

                }
                }
            }
        }

    def feed_handle(self):
        results = {}
        result_active_request = {}
        result_active_user = {}
        result_register_user = {}
        result_unregister_user = {}
        res_active_request = self.request_es(self.qsl_body_active_user_request)
        res_active_user = self.request_es(self.qsl_body_active_user)
        res_register_user = self.request_es(self.qsl_body_register_user_request)
        res_unregister_user = self.request_es(self.qsl_body_unregister_user_request)
        request_data = res_active_request["aggregations"]["request_per_day"]["buckets"]
        if len(request_data) == 2:
            yesterday_request_data = request_data[1]
        else:
            yesterday_request_data = request_data[0]
        print('debug active_requests data',yesterday_request_data)
        timestamp = yesterday_request_data["key_as_string"]
        active_requests = yesterday_request_data["active_requests"]["value"]
        print('active_requests at ',timestamp,'is',active_requests)
        result_active_request[timestamp] = active_requests
        #for result in res_active_user["aggregations"]["active_user_per_day"]["buckets"][1]:
        active_user_data = res_active_user["aggregations"]["active_user_per_day"]["buckets"]
        if len(active_user_data) == 2:
            yesterday_active_user_data = active_user_data[1]
        else:
            yesterday_active_user_data = active_user_data[0]
        print('debug total_active_user data',yesterday_active_user_data)
        timestamp = yesterday_active_user_data["key_as_string"]
        active_user = yesterday_active_user_data["total_active_user"]["value"]
        print('active_user at ',timestamp,'is',active_user)
        result_active_user[timestamp] = active_user
        #for result in res_register_user["aggregations"]["register_user_request_per_day"]["buckets"][1]:
        register_user_data = res_register_user["aggregations"]["register_user_request_per_day"]["buckets"]
        print(register_user_data)
        if len(register_user_data) == 2:
            yesterday_register_user_data = register_user_data[1]
        else:
            yesterday_register_user_data = register_user_data[0]
        print('debug total_register_user_request data',yesterday_register_user_data)
        timestamp = yesterday_register_user_data["key_as_string"]
        register_user_request = yesterday_register_user_data["total_register_user_request"]["value"]
        print('register_user_request at ',timestamp,'is',register_user_request)
        result_register_user[timestamp] = register_user_request
        #for result in res_unregister_user["aggregations"]["unregister_user_request_per_day"]["buckets"][1]:
        unregister_user_data = res_unregister_user["aggregations"]["unregister_user_request_per_day"]["buckets"]
        if len(unregister_user_data) == 2:
            yesterday_unregister_user_data = unregister_user_data[1]
        else:
            yesterday_unregister_user_data = unregister_user_data[0]
        print('debug total_unregister_user_request data',yesterday_unregister_user_data)
        timestamp =yesterday_unregister_user_data["key_as_string"]
        unregister_user_request = yesterday_unregister_user_data["total_unregister_user_request"]["value"]
        print('unregister_user_request ',timestamp,'is',unregister_user_request)
        result_unregister_user[timestamp] = unregister_user_request
        print('debug results before deal with it',result_active_user,result_active_request, \
            result_register_user,result_unregister_user)
        for key, value in result_active_user.items():
            time_stamp = key
            active_user = value
            print('debug active_user at ',time_stamp,'is',active_user)
            active_requests = result_active_request[time_stamp]
            register_user_request = result_register_user[time_stamp]
            unregister_user_request = result_unregister_user[time_stamp]
            print(time_stamp,active_requests,active_user,register_user_request,unregister_user_request)
            avg_active_user_requests = round(active_requests / active_user, 2)
            avg_register_user_requests = round(register_user_request / active_user, 2)
            avg_unregister_user_requests = round(unregister_user_request / active_user, 2)
            result_active_request[time_stamp] = [active_requests,active_user, avg_active_user_requests]
            result_register_user[time_stamp] = [register_user_request,active_user, avg_register_user_requests]
            result_unregister_user[time_stamp] = [unregister_user_request,active_user, avg_unregister_user_requests]
#        return result_one, result_two
        return result_active_request,result_register_user,result_unregister_user


if __name__ == "__main__":
    result_active_request,result_register_user,result_unregister_user = RegisterUserCount().feed_handle()
    for key, value in result_active_request.items():
        if key != "2018-08-18T00:00:00.000+08:00":
	    print key
            print value[0]
            print value[1]
            print value[2]
    for key, value in result_register_user.items():
        if key != "2018-08-18T00:00:00.000+08:00":
	    print key
	    print value[0]
            print value[1]
            print value[2]
    for key, value in result_unregister_user.items():
        if key != "2018-08-18T00:00:00.000+08:00":
	    print key
	    print value[0]
            print value[1]
            print value[2]
