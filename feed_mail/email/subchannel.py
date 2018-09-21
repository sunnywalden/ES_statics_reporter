# -*- coding: utf-8 -*
from base import base

from constant import PAGE_ID, PAGE_ID_NAME


class SubchannelCount(base):
    def __init__(self):
        super(SubchannelCount, self).__init__()
        self.gen_dict = dict(zip(PAGE_ID, PAGE_ID_NAME))

    def subchannel(self):
        click_dict = self.daily_click_data()
        result = {}
        for k, v in self.gen_dict.items():
            result[v] = str(self.page_dict[k]) + '|' + str(len(self.user_dict[k]))
        return result
