# -*- coding:utf-8 -*-
import time, json, uuid
from client import Client
from index import mapping_body
import codecs

class HandleFeed:
    def __init__(self):
        self.expire_time = 24 * 60 * 60
        self.feed_page_id = '96902993aee64246a44790f8f1cbab9c'
        self.timestamp = time.localtime(time.time() - 60 * 60)
        self.date = time.strftime('%Y%m%d%H', self.timestamp)
        self.index_date = time.strftime('%Y%m%d', self.timestamp)
        self.input_path = "/data/feed/feed_input_log/service.txt." + self.date
        self.click_path = '/data/page_source/click/' + time.strftime('%Y/%m/%d/%H', self.timestamp) + '.txt'
        self.host = ['10.200.26.28:27017', '10.200.26.22:27017', '10.200.26.29:27017']
        self.replicaset = "migu_video"
        self.profile_host = ['10.150.28.53:21000', '10.150.28.54:21000', '10.150.28.45:21000']
        self.profile_database = "profile"
        self.profile_collection = "item_profile"
        self.index = 'feed_v1-' + self.index_date
        self.type = 'logs'
        self.client = Client()
        self.profile = 'profile-'
        self.bulk_num = 3000
        self.dict_field = ['contDuration', 'contDisplayType', 'contDisplayName', 'mediaShape', 'mediaArea',
                           'mediaType', 'packId', 'publishTime', 'createTime', 'CP_ID', 'mediaTime']
        self.new_dict_field = ['contDuration', 'contDisplayType', 'contDisplayName', 'mediaShape', 'mediaArea',
                               'mediaType', 'packId', 'publishTime', 'createTime', 'CP_ID', 'mediaTime', 'keywords']
        self.res_field = ['user_id', 'ctinfo', 'page_source', 'msisdn', 'client_id', 'channel']
        self.return_dict = {'searchId': 1, 'mediaShape': 1, 'packId': 1, 'contRecomm': 1, 'mediaType': 1,
                            'publishTime': 1, 'contDisplayType': 1, 'contDisplayName': 1, '_id': 0, 'contDuration': 1,
                            'createTime': 1, 'mediaArea': 1, 'CP_ID': 1, 'mediaTime': 1, 'source': 1}
        self.profile_return_dict = {'_id':0,'searchId': 1, 'contName': 1, 'tag_info_list': 1}
        self.channel_dict = {'hotspot-picked':'热点', 'hotspot-hottest':'热播', 'hotspot-life':'生活', 'hotspot-comedy': '搞笑', 'hotspot-game': '游戏', 'hotspot-music':'音乐'}

    def hour_click_data(self):
        click_dict, active_user = {}, set()
        with open(self.click_path, 'r') as fs:
            for f in fs:
                line = f.split('|')
                if line[0] == self.feed_page_id:
                    if line[5]: active_user.add(line[5])
                    client_id = line[5] if line[5] else '0000'
                    click_dict[client_id + '|' + line[4][0:9]] = line[6]
        return click_dict, active_user

    def handle_feed(self):
        count = 0
        qsl_body = []
        click_dict, active_user = self.hour_click_data()
        with open(self.input_path, 'r') as lines:
            for line in lines:
                res = {}
                line = line.strip()
                c = json.loads(line.split('SHOW_FOR_ANALYSIS:')[1])
                self.first_fields(active_user, c, line, res)
                for i in c['feed_items']:
                    count += 1
                    dic = {}
                    self.second_fields(c, click_dict, dic, i)
                    qsl_body.append({"index": {"_id": str(uuid.uuid1())}})
                    qsl_body.append(dict(res, **dic))
                    if count % self.bulk_num == 0:
                        self.request_es(qsl_body)
                        qsl_body = []
            self.request_es(qsl_body)

    def second_fields(self, c, click_dict, dic, i):
        dic["search_id"] = i[0]
        value = self.get_video_data(i[0])
        if isinstance(value, list) and len(value) > 0: value = value[0]
        #for key in self.dict_field:
        for key in self.new_dict_field:
            dic[key] = value.get(key)
        dic['video_source'] = value.get('source')
        re = value.get('contRecomm')
        dic['contRecomm'] = re.encode("utf-8").replace('，', ',').decode('utf-8').split(',') if re else re
        dic['source'] = i[1].split(' ')[0]
        source_tag = i[1].split(':')[0].split(' ')
        dic['tag'] = source_tag[0].split('_')[1] if '_' in source_tag[0] else source_tag[1]
        dic['click'] = 0
        dic['watch_time'] = None
        key = c['client_id'] + '|' + i[0] if c['client_id'] else '0000' + '|' + i[0]
        if key in click_dict:
            dic['click'] = 1
            dic['watch_time'] = click_dict[key]

    def first_fields(self, active_user, c, line, res):
        res['timeLine'] = line[0:19].replace(' ', 'T') + '.000+0800'
        res['timeLocal'] = line[0:19].replace(' ', 'T') + '.000Z'
        res['timeStamp'] = self.modify_time(line[0:19])
        for key in self.res_field:
            #if key == 'channel':
            #    res[key] = 
            res[key] = c[key]
        source = set([i[1].split(' ')[0] for i in c['feed_items']])
        res['four_source'] = 1 if len(source) == 4 else 0
        #if len(source) == 3 and 'ig_hot' in source and 'ig_hot' in source and 'ir' in source:
        #    res['four_source'] = 1
        res['profile'] = 1 if 'ir' in source else 0
        res['request_active'] = 1 if c['client_id'] in active_user else 0
        res['register_user'] = self.judge_register_user(c['client_id'])
        res['request'] = str(uuid.uuid1())

    def get_video_data(self, search_ids):
        #redis = self.client.get_redis_client()
        redis = self.client.get_redis_new_client()
        keywords_redis = get_redis_keywords_client()
        if redis.get(search_ids):
            value = json.loads(redis.get(search_ids))
        else:
            value = self.get_mongo_video(search_ids)
            if keywords_redis.get(search_ids):
                keywords_dict = json.loads(keywords_redis.get(search_ids))
                value[0]['keywords'] = keywords_dict[search_ids]
            else:
                keyword_value = self.get_mongo_profile(search_ids)
            #print('debug return of program mongo',type(value),value)
            #print('debug return of item_profile mongo',type(keyword_value),keyword_value)
                if isinstance(keyword_value, list) and len(keyword_value) > 0:
                    keywords = keyword_value[0]['tag_info_list']
                    kwords = []
                    for keyword in keywords:
                        kwords.append(keyword['tag'])
                    value[0]['keywords'] = kwords
                    keywords_redis.set(search_ids, json.dumps(kwords), ex=self.expire_time)
                else:
                    print('No item profile found for', search_ids)
            #print('debug value before storage to redis',value)

            #print('debug value before return',value)            
            redis.set(search_ids, json.dumps(value), ex=self.expire_time)
        return value

    def get_mongo_video(self, search_id):
        lines = self.client.get_mongo_client(self.host, self.replicaset).find({'searchId': search_id}, self.return_dict,
                                                                              no_cursor_timeout=True)
        return [line for line in lines]

    def get_mongo_profile(self, search_id):
        lines = self.client.get_profile_mongo_client(self.profile_host, self.profile_database, self.profile_collection).find({'searchId': search_id}, self.profile_return_dict,no_cursor_timeout=True)
        return [line for line in lines]
    
    def request_es(self, qsl_body):
        self.client.es_conn.bulk(index=self.index, doc_type=self.type, body=qsl_body, request_timeout=90)

    def create_index(self):
        if not self.client.es_conn.indices.exists(index=self.index):
            self.client.es_conn.indices.create(self.index, body=mapping_body)

    def judge_register_user(self, client_id):
        register = 1 if self.client.get_redis_cluster_client().get(self.profile + client_id) else 0
        return register

    def modify_time(self, time_str):
        tim = time.mktime(time.strptime(time_str, '%Y-%m-%d %H:%M:%S'))
        front_time = time.localtime(tim - 8 * 3600)
        new_time = time.strftime("%Y-%m-%d %H:%M:%S", front_time)
        return new_time


if __name__ == '__main__':
    handle = HandleFeed()
    handle.create_index()
    handle.handle_feed()
    # try:
    #    handle.handle_feed()
    # except:
    #    print 'An error occurred'
