# -*- coding:utf-8 -*-

import json
from base import base
from redis_client import RedisClient


class RegisterUserCount(base):
    def __init__(self):
        super(RegisterUserCount, self).__init__()
        self.profile = 'profile-'
        self.redis = RedisClient().get_redis_client()
        self.sets = set()

    def judge_register_user(self, client_id):
        key = self.profile + client_id
        if key in self.sets:
            return 1
        else:
            value = self.redis.get(key)
            if value:
                self.sets.add(value)
                return 1
        return 0

    def feed_handle(self):
        active_request, active_users = 0, set()
        register_request, register_users = 0, set()
        no_register_request, no_register_users = 0, set()
        click_dict, active_user = self.daily_click_data()
        for line in self.daily_feed_data():
            items = json.loads(line)
            client_id = items['client_id'] if items['client_id'] else '0000'
            if client_id in active_user:
                active_request += 1
                active_users.add(client_id)
                register = self.judge_register_user(client_id)
                if register == 1:
                    register_request += 1
                    register_users.add(client_id)
                if register == 0:
                    no_register_request += 1
                    no_register_users.add(client_id)
        active_tuple = (active_request, active_users, self.calc_average_value(active_request, active_users))
        register_tuple = (register_request, register_users, self.calc_average_value(register_request, register_users))
        no_register_tuple = (
            no_register_request, no_register_users, self.calc_average_value(no_register_request, no_register_users))
        return active_tuple, register_tuple, no_register_tuple

    def feed_handle_click(self):
        register_click = {key: 0 for key in ['ig_new', 'ig_hot', 'ir', 'tsg']}
        on_register_click = {key: 0 for key in ['ig_new', 'ig_hot', 'ir', 'tsg']}
        click_dict, active_user = self.daily_click_data()
        for line in self.daily_feed_data():
            items = json.loads(line)
            client_id = items['client_id'] if items['client_id'] else '0000'
            register = self.judge_register_user(client_id)
            for feed in items['feed_items']:
                source = feed[1].split(' ')[0]
                key = client_id + '|' + feed[0]
                if key in click_dict:
                    if register == 1:
                        register_click[source] += 1
                    if register == 0:
                        on_register_click[source] += 1
        return register_click, on_register_click

    def calc_average_value(self, x, y):
        return '%.2f' % (float(x) / float(len(y)))
