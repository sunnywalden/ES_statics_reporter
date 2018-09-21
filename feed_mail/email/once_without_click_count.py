# -*- coding:utf-8 -*-

import json
from base import base


class OnceWithoutClickCount(base):
    def feed_handle(self):
        exposure = {key: 0 for key in ['ig_new', 'ig_hot', 'ir', 'tsg']}
        click = {key: 0 for key in ['ig_new', 'ig_hot', 'ir', 'tsg']}
        user = {key: set() for key in ['ig_new', 'ig_hot', 'ir', 'tsg']}
        click_dict, active_user = self.daily_click_data()
        for line in self.daily_feed_data():
            items = json.loads(line)
            client_id = items['client_id'] if items['client_id'] else '0000'
            if client_id not in active_user:
                continue
            for feed in items['feed_items']:
                source = feed[1].split(' ')[0]
                exposure[source] += 1
                key = client_id + '|' + feed[0]
                if key in click_dict:
                    click[source] += 1
                    user[source].add(client_id)
        return exposure, click, user
