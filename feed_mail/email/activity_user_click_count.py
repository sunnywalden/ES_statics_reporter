# -*- coding:utf-8 -*-

import json
from base import base


class ActivityUserClickCount(base):
    def feed_handle(self):
        click = 0
        user = set()
        click_dict, active_user = self.daily_click_data()
        for line in self.daily_feed_data():
            items = json.loads(line)
            client_id = items['client_id'] if items['client_id'] else '0000'
            for feed in items['feed_items']:
                key = client_id + '|' + feed[0]
                if key in click_dict:
                    click += 1
                    user.add(client_id)
        average_click = '%.2f' % (float(click) / float(len(user)))
        return click, user, average_click
