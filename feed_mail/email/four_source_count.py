# -*- coding:utf-8 -*-

import json
from base import base


class FourSourceCount(base):
    def feed_handle(self):
        exposure = {key: 0 for key in ['ig_new', 'ig_hot', 'ir', 'tsg']}
        click = {key: 0 for key in ['ig_new', 'ig_hot', 'ir', 'tsg']}
        user = {key: set() for key in ['ig_new', 'ig_hot', 'ir', 'tsg']}
        click_dict, active_user = self.daily_click_data()
        for line in self.daily_feed_data():
            items = json.loads(line)
            client_id = items['client_id'] if items['client_id'] else '0000'
            set_source = set()
            for feed in items['feed_items']:
                source = feed[1].split(' ')[0]
                set_source.add(source)
            if len(set_source) == 4:
                self.set_value(click, click_dict, client_id, exposure, items, user)
#            if len(set_source) == 3 and 'ig_hot' in set_source and 'ig_hot' in set_source and 'ir' in set_source:
#                self.set_value(click, click_dict, client_id, exposure, items, user)
        return exposure, click, user

    def set_value(self, click, click_dict, client_id, exposure, items, user):
        for feed in items['feed_items']:
            source = feed[1].split(' ')[0]
            exposure[source] += 1
            key = client_id + '|' + feed[0]
            if key in click_dict:
                click[source] += 1
                user[source].add(client_id)
