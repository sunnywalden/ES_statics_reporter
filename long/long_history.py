# -*- coding:utf-8 -*-
import time, json, re, uuid, os
from multiprocessing import Process
from client import Client
from index import mapping_body
import re

class handleLongLog:
    def __init__(self,timestamp):
	self.day = time.strftime('%Y%m%d', timestamp)
	self.time = time.strftime('%Y%m%d%H', timestamp)
        #self.time = time.strftime('%Y%m%d%H', time.localtime(time.time() - 60 * 60))
        self.nginx_input_path = "/data/long/nginx_log/service.txt." + self.time
        self.api_input_path = "/data/long/long_input_log/service.txt." + self.time
        self.click_input_path = "/data/click/service.txt." + self.time
        self.res_field = ['user_id', 'ctinfo', 'ct', 'msisdn', 'client_id', 'search_id']
        self.dict_field = ['contDuration', 'contDisplayType', 'mediaShape', 'contRecomm',
                           'mediaArea', 'mediaType', 'packId', 'publicTime', 'createTime']
        #self.index = 'long-v1-' + time.strftime('%Y%m%d', time.localtime(time.time()))
        self.index = 'long-v1-' + self.day
        self.type = 'logs'
        self.client = Client()
        self.expire_time = 24 * 60 * 60
        self.host = ['10.200.26.28:27017', '10.200.26.22:27017', '10.200.26.29:27017']
        self.replicaset = "migu_video"

    def read_api_data(self):
        result_dict = {}
        with open(self.api_input_path, 'r') as lines:
            for line in lines:
                line = line.strip()
                if re.findall(r'SHOW_FOR_ANALYSIS', line):
                    cont = line.split('SHOW_FOR_ANALYSIS:')[1].replace('None', 'null')
                    try: 
                        recomm_dict = json.loads(cont)
                    except ValueError:
                        print('deal with message wrong')
                        continue
                        #ct = cont.replace('\\', '')
                        #recomm_dict = json.loads(ct.decode('utf-8'))
                    #recomm_dict = eval(cont)
                    recomm_dict['time'] = line[0:19]
                    recomm_dict['timeLine'] = line[0:19].replace(' ', 'T') + '.000+0800'
                    recomm_dict['timeLocal'] = line[0:19].replace(' ', 'T') + '.000Z'
                    recomm_dict['timeStamp'] = self.modify_time(line[0:19])
                    client_id = recomm_dict['client_id'] if recomm_dict['client_id'] else '0000'
                    key = client_id + recomm_dict['search_id']
                    #hash_key = hash(key)
                    if key in result_dict:
                        result_dict[key].append(recomm_dict)
                    else:
                        result_dict[key] = [recomm_dict]
        return result_dict

    def read_click_data(self):
        result_dict = {}
        with open(self.click_input_path, 'r') as lines:
            for line in lines:
                line = line.strip()
                cont = line.split(',')
                client_id = cont[4] if cont[4] else '0000'
                key = client_id + cont[3]
                value = {'click': cont[0], 'watch_time': cont[8]}
                #hash_key = hash(key)
                if key in result_dict:
                    #if key in result_dict[hash_key]:
                    result_dict[key].append(value)
                    #else:
                    #    result_dict[hash_key][key] = [value]
                else:
                    result_dict[key] = [value]
        return result_dict

    def get_video_data(self, search_ids):
        redis = self.client.get_redis_client()
        if redis.get(search_ids):
            try:
                value = json.loads(redis.get(search_ids))
            except TypeError:
                print('the return of redis is nomal', redis.get(search_ids))
                value = self.get_mongo_video(search_ids)
        else:
            value = self.get_mongo_video(search_ids)
            redis.set(search_ids, json.dumps(value), ex=self.expire_time)
        return value

    def get_mongo_video(self, search_id):
        mongo_client = self.client.get_mongo_client(self.host, self.replicaset)
        return_dict = {'searchId': 1, 'mediaShape': 1, 'packId': 1, 'contRecomm': 1, 'mediaType': 1, 'publicTime': 1,
                       'contDisplayType': 1, '_id': 0, 'contDuration': 1, 'createTime': 1, 'mediaArea': 1}
        lines = mongo_client.find({'searchId': search_id}, return_dict, no_cursor_timeout=True)
        return [line for line in lines]

    def modify_time(self, time_str):
        tim = time.mktime(time.strptime(time_str, '%Y-%m-%d %H:%M:%S'))
        front_time = time.localtime(tim - 8 * 3600)
        new_time = time.strftime("%Y-%m-%d %H:%M:%S", front_time)
        return new_time

    def run(self, total_line, click_line):
        count = 0
        res = {}
        qsl_body = []
        #for k, v in value.items():
           #if k in total_line:
        for k, v in total_line.items():
                #line = total_line[k].split("||")
                #res['time'] = line[0]
            for record in v:
                content = record
                #print('debug one recocmmed result ', content)
                #content = json.loads(v)
                #res['time'] = content['time']
		res['timeStamp'] = content['timeStamp']
                res['timeLocal'] = content['timeLocal']
                res['timeLine'] = content['timeLine']
                res['source'] = content['item_source']	
                for key in self.res_field:
                    res[key] = content[key]
                for i in content['related_items']:
                    #print('debug one of recommend result before deal with it', i)
                    rec_record = i
                    total = None
                    click_dict = {}
                    search_ids = rec_record['search_id']
                    client_id = content['client_id']
                    dic = {'search_id': search_ids, 'click': 0, 'watch_time': None}
                    value = self.get_video_data(search_ids)
                    if isinstance(value, list) and len(value) > 0: value = value[0]
                    for key in self.dict_field:
                        dic[key] = value.get(key) if value else None
                    #for n in range(len(v)):
                    count += 1

                    click_key = client_id + search_ids if client_id else '0000' + search_ids
                    if click_key in click_line:
                        data = click_line[click_key]
                        # total = len(data)
                        click_dict = {'click': 1, 'watch_time': data[0]['watch_time']}
                        dic['click'] = 1
                        dic['watch_time'] = click_dict['watch_time']
                    #if n < total:
                    #    dic = dict(dic, **click_dict)
                    qsl_body.append({"index": {"_id": str(uuid.uuid1())}})
                    qsl_body.append(dict(res, **dic))
                    if count % 50 == 0:
                        self.request_es(qsl_body)
                        qsl_body = []
        self.request_es(qsl_body)

    def request_es(self, qsl_body):
        try:
            self.client.es_conn.bulk(index=self.index, doc_type=self.type, body=qsl_body, request_timeout=120)
        except Exception as e:
            print '################################################'
            print qsl_body

    def create_index(self):
        if not self.client.es_conn.indices.exists(index=self.index):
            self.client.es_conn.indices.create(self.index, body=mapping_body)


if __name__ == '__main__':
    start_time = time.strftime('%Y%m%d %X', time.localtime(time.time()))
    print('start log of loast hour storage',start_time)
    hours = range(21, 22)
    for hour in reversed(hours):
        new_time = (2018, 9, 16, hour, 0, 0, 0, 0, 0)
        timestamp = time.localtime(time.mktime(new_time))
        print 'input_path' + time.strftime('%Y%m%d%H', timestamp)
        handle = handleLongLog(timestamp)
        handle.create_index()
    #nginx_data = handle.read_nginx_data()
        api_data = handle.read_api_data()
        click_data = handle.read_click_data()
    #print(len(nginx_data), len(api_data), len(click_data))
    #print(len(api_data), len(click_data))
    #print 'Parent process %s.' % os.getpid()
    #for key, value in nginx_data.items():
        #print(key, value)
    #if len(str(key)) > 8:
    #for key, value in api_data.items():
        handle.run(api_data, click_data)
        #print(key, value)
        #p = Process(target=handle.run, args=(value, api_data[key], click_data[key],))
        #p.start()
    #print 'Waiting for all subprocesses done...'
    finish_time = time.strftime('%Y%m%d %X', time.localtime(time.time()))
    print('finished at',finish_time)
    print 'All processes done.'
