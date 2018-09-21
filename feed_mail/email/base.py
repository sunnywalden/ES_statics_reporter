# -*- coding:utf-8 -*-

import time
import os
from constant import FEED_PAGE_ID, PAGE_ID


class base(object):
    def __init__(self):
        self.timestamp = time.localtime(time.time() - 24 * 60 * 60)
        self.month = time.strftime('%Y%m', self.timestamp)
        self.day = time.strftime('%Y%m%d', self.timestamp)
        self.feed_path = '/mnt/feed/RCMD-11101-RecommendLog/' + self.month + '/' + self.day + '/'
        self.click_path = '/data/page_source/click/' + time.strftime('%Y/%m/%d/', self.timestamp)
        self.page_dict = {id: 0 for id in PAGE_ID}
        self.user_dict = {id: set() for id in PAGE_ID}

    def daily_click_data(self):
        click_dict, active_user = {}, set()
        file_list = os.listdir(self.click_path)
        for file in file_list:
            with open(self.click_path + file, 'r') as fs:
                for f in fs:
                    line = f.split('|')
                    if line[0] in PAGE_ID:
                        self.page_dict[line[0]] += 1
                        if line[5]: self.user_dict[line[0]].add(line[5])
                    elif line[0] == FEED_PAGE_ID:
                        if line[5]: active_user.add(line[5])
                        client_id = line[5] if line[5] else '0000'
                        click_dict[client_id + '|' + line[4][0:9]] = 1
        return click_dict, active_user

    def daily_feed_data(self):
        result_list = []
        file_list = os.listdir(self.feed_path)
        file_list = filter(lambda x: 'verf' not in x, file_list)
        for file in file_list:
            with open(self.feed_path + file, 'r') as fs:
                for f in fs:
                    result_list.append(f.strip().split('|')[1])
        return result_list
