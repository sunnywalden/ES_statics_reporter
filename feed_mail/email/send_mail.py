# -*- coding: utf-8 -*
import json
import socket
import urllib2
from datetime import date, timedelta

from activity_user_click_count import ActivityUserClickCount
from click_and_four_source_count import ClickAndFourSourceCount
from feed_count import FeedCount
from four_source_count import FourSourceCount
from once_without_click_count import OnceWithoutClickCount
from register_user_count import RegisterUserCount
from subchannel import SubchannelCount


class sendMail:
    def __init__(self):
        self.day = (date.today() - timedelta(1)).strftime("%Y年%m月%d日")
        self.sender = "no-reply-devops@cloudin.cn"  # 发件人邮箱
        self.password = "migu1qaz!QAZ"  # 发件人密码
        self.subject = "统计结果：%s-feed和子频道统计" % self.day  # 邮件标题
        self.mail_server_url = 'http://10.200.26.14:5000/sendmail'
        self.receivers = ['shanghai@cloudin.cn', 'sangyongjia@migu.cn', 'wangdongguang@migu.cn']
        #self.receivers = ['zhangbo@cloudin.cn']
        # self.receivers = ['gouhaiding@cloudin.cn', 'huangfuxin@cloudin.cn']
        self.fromer = 'no-reply-devops@cloudin.cn'

    def http_post(self, html):
        html = '<p>主机：%s</p><p></p>' % socket.gethostname() + html
        data = {
            "smtpserver": {
                "mailpasswd": "migu1qaz!QAZ",
                "mailuser": "no-reply-devops@cloudin.cn",
                "server": "smtp.exmail.qq.com"
            },
            "fromuser": {
                "fromer": self.fromer
            },
            "receivesuser": {
                "receiver": self.receivers
            },
            "emailsubject": {
                "mess": html,
                "subject": "统计结果：" + self.day + "feed和子频道统计"
            }
        }
        print data
        headers = {'Content-Type': 'application/json'}
        req = urllib2.Request(url=self.mail_server_url, headers=headers, data=json.dumps(data))
        response = urllib2.urlopen(req)
        return response.read()

    def calc_average_value(self, x, y):
        return '%.2f' % (float(x) / float(len(y)))

    def total_count(self, html):
        exposure, click, user = FeedCount().feed_handle()
        feed_titile = '<tr><td rowspan="6">总体</td><td></td><td>曝光量</td><td>点击量</td><td>去重用户</td><td>点击率</td></tr>'
        html += feed_titile
        for key in ['ig_new', 'ig_hot', 'ir', 'tsg']:
            if exposure[key] == 0:
                html += '<tr><td>' + key + '</td><td>' + '0' + '</td><td>' + '0' + '</td><td>' + '0' + '</td><td>' + '0.00%' + '</td></tr>'
            else:
                html += '<tr><td>' + key + '</td><td>' + str(exposure[key]) + '</td><td>' + str(
                    click[key]) + '</td><td>' + str(len(user[key])) + '</td><td>' + "%.2f%%" % (
                    float(click[key]) / float(exposure[key]) * 100) + '</td></tr>'
        exprose_sum = sum(exposure.itervalues())
        click_sum = sum(click.itervalues())
        html += '<tr><td>总计</td><td>' + str(exprose_sum) + '</td><td>' + str(click_sum) + '</td><td>' + str(
            sum([len(v) for v in user.values()])) + '</td><td>' + '{:.2%}'.format(
            float(click_sum) / float(exprose_sum)) + '</td></tr>'
        return html

    def once_without_click(self, html):
        exposure, click, user = OnceWithoutClickCount().feed_handle()
        click_title = '<tr><td rowspan="6">过滤一次未点击</td><td></td><td>曝光量</td><td>点击量</td><td>去重用户</td><td>点击率</td></tr>'
        html += click_title
        for key in ['ig_new', 'ig_hot', 'ir', 'tsg']:
            if exposure[key] == 0:
                html += '<tr><td>' + key + '</td><td>' + '0' + '</td><td>' + '0' + '</td><td>' + '0' + '</td><td>' + '0.00%' + '</td></tr>'
            else:
                html += '<tr><td>' + key + '</td><td>' + str(exposure[key]) + '</td><td>' + str(
                    click[key]) + '</td><td>' + str(len(user[key])) + '</td><td>' + '{:.2%}'.format(
                    float(click[key]) / float(exposure[key])) + '</td></tr>'
        once_exprose = sum(exposure.itervalues())
        once_click = sum(click.itervalues())
        html += '<tr><td>总计</td><td>' + str(once_exprose) + '</td><td>' + str(once_click) + '</td><td>' + str(
            sum([len(v) for v in user.values()])) + '</td><td>' + '{:.2%}'.format(
            float(once_click) / float(once_exprose)) + '</td></tr>'
        return html

    def four_source(self, html):
        exposure, click, user = FourSourceCount().feed_handle()
        source_title = '<tr><td rowspan="6">存在四种来源</td><td></td><td>曝光量</td><td>点击量</td><td>去重用户</td><td>点击率</td></tr>'
        html += source_title
        for key in ['ig_new', 'ig_hot', 'ir', 'tsg']:
            if exposure[key] == 0:
                html += '<tr><td>' + key + '</td><td>' + '0' + '</td><td>' + '0' + '</td><td>' + '0' + '</td><td>' + '0.00%' + '</td></tr>'
            else:
                html += '<tr><td>' + key + '</td><td>' + str(exposure[key]) + '</td><td>' + str(
                    click[key]) + '</td><td>' + str(len(user[key])) + '</td><td>' + '{:.2%}'.format(
                    float(click[key]) / float(exposure[key])) + '</td></tr>'
        normal_exprose = sum(exposure.itervalues())
        normal_click = sum(click.itervalues())
        html += '<tr><td>总计</td><td>' + str(normal_exprose) + '</td><td>' + str(normal_click) + '</td><td>' + str(
            sum([len(v) for v in user.values()])) + '</td><td>' + '{:.2%}'.format(
            float(normal_click) / float(normal_exprose)) + '</td></tr>'
        return html

    def once_click_and_four_source(self, html):
        exposure, click, user = ClickAndFourSourceCount().feed_handle()
        click_four_title = '<tr><td rowspan="6">过滤一次未点<br/>击用户曝光且<br/>存在四种来源</td><td></td><td>曝光量</td><td>点击量</td><td>去重用户</td><td>点击率</td></tr>'
        html += click_four_title
        for key in ['ig_new', 'ig_hot', 'ir', 'tsg']:
            if exposure[key] == 0:
                html += '<tr><td>' + key + '</td><td>' + '0' + '</td><td>' + '0' + '</td><td>' + '0' + '</td><td>' + '0.00%' + '</td></tr>'
            else:
                html += '<tr><td>' + key + '</td><td>' + str(exposure[key]) + '</td><td>' + str(
                    click[key]) + '</td><td>' + str(len(user[key])) + '</td><td>' + '{:.2%}'.format(
                    float(click[key]) / float(exposure[key])) + '</td></tr>'
        normal_exprose = sum(exposure.itervalues())
        normal_click = sum(click.itervalues())
        html += '<tr><td>总计</td><td>' + str(normal_exprose) + '</td><td>' + str(normal_click) + '</td><td>' + str(
            sum([len(v) for v in user.values()])) + '</td><td>' + '{:.2%}'.format(
            float(normal_click) / float(normal_exprose)) + '</td></tr>'
        return html

    def activity_user_click(self, html):
        click, user, average_click = ActivityUserClickCount().feed_handle()
        user_title = '<tr><td rowspan="2">活跃用户-点击</td><td></td><td>活跃用户点击量</td><td>活跃用户量</td><td>平均点击次数</td><td align="center">-</td></tr>'
        html += user_title
        html += '<tr><td>总计</td><td>' + str(click) + '</td><td>' + str(
            len(user)) + '</td><td>' + average_click + '</td><td align="center">-</td></tr>'
        return html

    def register_user_count(self, request, html):
        active_tuple, register_tuple, no_register_tuple = request
        active_title = '<tr><td rowspan="2">活跃用户-请求</td><td></td><td>活跃用户请求量</td><td>活跃用户量</td><td>平均请求次数</td><td align="center">-</td></tr>'
        registered_title = '<tr><td rowspan="2">注册</td><td></td><td>注册用户请求量</td><td>活跃用户量</td><td>平均请求次数</td><td align="center">-</td></tr>'
        no_registered_title = '<tr><td rowspan="2">未注册</td><td></td><td>未注册用户请求量</td><td>活跃用户量</td><td>平均请求次数</td><td align="center">-</td></tr>'
        html += active_title
        html += '<tr><td>总计</td><td>' + str(active_tuple[0]) + '</td><td>' + str(len(active_tuple[1])) + '</td><td>' + \
                active_tuple[2] + '</td><td align="center">-</td></tr>'
        html += registered_title
        html += '<tr><td>总计</td><td>' + str(register_tuple[0]) + '</td><td>' + str(
            len(register_tuple[1])) + '</td><td>' + register_tuple[2] + '</td><td align="center">-</td></tr>'
        html += no_registered_title
        html += '<tr><td>总计</td><td>' + str(no_register_tuple[0]) + '</td><td>' + str(
            len(no_register_tuple[1])) + '</td><td>' + no_register_tuple[2] + '</td><td align="center">-</td></tr>'
        return html

    def register_user_click_count(self, click, html):
        register_click, on_register_click = click
        registered_title = '<tr><td rowspan="6">注册<br/>区分来源</td><td></td><td>点击量</td><td align="center">-</td><td align="center">-</td><td align="center">-</td></tr>'
        no_registered_title = '<tr><td rowspan="6">未注册<br/>区分来源</td><td></td><td>点击量</td><td align="center">-</td><td align="center">-</td><td align="center">-</td></tr>'
        html += registered_title
        for key in ['ig_new', 'ig_hot', 'ir', 'tsg']:
            if register_click.get(key):
                register_s = register_click.get(key)
            else:
                register_s, register_click[key] = 0, 0
            html += '<tr><td>' + key + '</td><td>' + str(
                register_s) + '</td><td align="center">-</td><td align="center">-</td><td align="center">-</td></tr>'
        html += '<tr><td>总计</td><td>' + str(
            reduce(lambda x, y: x + y, [register_click[k] for k in ['ig_new', 'ig_hot', 'ir',
                                                                    'tsg']])) + '</td><td align="center">-</td><td align="center">-</td><td align="center">-</td></tr>'
        html += no_registered_title
        for key in ['ig_new', 'ig_hot', 'ir', 'tsg']:
            if on_register_click.get(key):
                no_register_s = on_register_click[key]
            else:
                no_register_s, on_register_click[key] = 0, 0
            html += '<tr><td>' + key + '</td><td>' + str(
                no_register_s) + '</td><td align="center">-</td><td align="center">-</td><td align="center">-</td></tr>'
        html += '<tr><td>总计</td><td>' + str(
            reduce(lambda x, y: x + y, [on_register_click[k] for k in ['ig_new', 'ig_hot', 'ir',
                                                                       'tsg']])) + '</td><td align="center">-</td><td align="center">-</td><td align="center">-</td></tr>'
        return html

    def subchannel_count(self, html):
        result = SubchannelCount().subchannel()
        sub_head_titile = '<tr><td colspan="6" align="center" bgcolor="#FF0000">子频道-数据统计</td></tr>'
        sub_titile = '<tr><td rowspan="15">子频道</td><td></td><td>点击量</td><td>去重用户</td><td align="center">-</td><td align="center">-</td></tr>'
        html += sub_head_titile
        html += sub_titile
        for k, v in result.items():
            value = v.split('|')
            html += '<tr><td>' + k + '</td><td>' + str(value[0]) + '</td><td>' + str(
                value[1]) + '</td><td align="center">-</td><td align="center">-</td></tr>'
        html += '<tr><td>总计</td><td>' + str(
            sum([int(v.split('|')[0]) for v in result.values()])) + '</td><td>' + str(
            sum([int(v.split('|')[1]) for v in
                 result.values()])) + '</td><td align="center">-</td><td align="center">-</td></tr>'
        html += '</table>'
        return html

    def send_msg(self):
        register = RegisterUserCount()
        head_title = '<tr><td colspan="6" align="center" bgcolor="#FF0000">热点-精选频道-推荐效果数据统计</td></tr>'
        html = '<table border="1" >' + head_title
        html = self.total_count(html)
        html = self.once_without_click(html)
        html = self.four_source(html)
        html = self.once_click_and_four_source(html)
        html = self.activity_user_click(html)
        html = self.register_user_count(register.feed_handle(), html)
        html = self.register_user_click_count(register.feed_handle_click(), html)
        html = self.subchannel_count(html)
        self.http_post(html)


if __name__ == '__main__':
    send_mail = sendMail()
    send_mail.send_msg()
