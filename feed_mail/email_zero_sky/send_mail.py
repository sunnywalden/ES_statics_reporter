# -*- coding: utf-8 -*
import json
import socket
import urllib2
from datetime import date, timedelta

from zero_day import DayClickRate
from zero_once_day import DayOnceClickRate
from zero_sources_day import DaySourceClickRate
from zero_sources_once_day import DaySourceOnceClickRate
from zero_register_day import DayRegisterClickRate


class sendMail:
    def __init__(self):
        self.day = (date.today() - timedelta(1)).strftime("%Y年%m月%d日")
        self.key = (date.today() - timedelta(2)).strftime("%Y-%m-%d") + 'T00:00:00.000+08:00'
        self.sender = "no-reply-devops@cloudin.cn"  # 发件人邮箱
        self.password = "migu1qaz!QAZ"  # 发件人密码
        self.subject = "统计结果：%s-feed-0元包-天脉统计" % self.day  # 邮件标题
        self.mail_server_url = 'http://10.200.26.14:5000/sendmail'
        self.receivers = ['shanghai@cloudin.cn', 'sangyongjia@migu.cn', 'wangdongguang@migu.cn']
        #self.receivers = ['zhangbo@cloudin.cn']
	# self.receivers = ['gouhaiding@cloudin.cn']
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
                "subject": "统计结果：" + self.day + "feed-0元包-天脉统计"
            }
        }
        print data
        headers = {'Content-Type': 'application/json'}
        req = urllib2.Request(url=self.mail_server_url, headers=headers, data=json.dumps(data))
        response = urllib2.urlopen(req)
        return response.read()

    def calc_average_value(self, x, y):
        if float(len(y)) != float(0):
            return '%.2f' % (float(x) / float(len(y)))
        else:
            return '%.2f' % (float(0))

    def total_count(self, html):
        result_one, result_two = DayClickRate().feed_handle()
        for key, value in result_one.items():
            if key != self.key:
                exposure, click, user, rate = value[0], value[1], result_two[key], value[2]
                feed_titile = '<tr><td rowspan="6">总体</td><td></td><td>曝光量</td><td>点击量</td><td>去重用户</td><td>点击率</td></tr>'
                html += feed_titile
                for key in ['ig_new', 'ig_hot', 'ir', 'tsg']:
                    if exposure[key] == 0:
                        html += '<tr><td>' + key + '</td><td>' + '0' + '</td><td>' + '0' + '</td><td>' + '0' + '</td><td>' + '0.00%' + '</td></tr>'
                    else:
                        html += '<tr><td>' + key + '</td><td>' + str(exposure[key]) + '</td><td>' + str(
                            click[key]) + '</td><td>' + str(user[key]) + '</td><td>' + rate[key] + '</td></tr>'
                exprose_sum = sum(exposure.itervalues())
                click_sum = sum(click.itervalues())
                if float(exprose_sum) != float(0):
                    html += '<tr><td>总计</td><td>' + str(exprose_sum) + '</td><td>' + str(click_sum) + '</td><td>' + str(
                       sum(user.itervalues())) + '</td><td>' + '{:.2%}'.format(
                       float(click_sum) / float(exprose_sum)) + '</td></tr>'
                else:
                    html += '<tr><td>总计</td><td>' + str(exprose_sum) + '</td><td>' + str(click_sum) + '</td><td>' + str(
                       sum(user.itervalues())) + '</td><td>' + '{:.2%}'.format(
                       float(0)) + '</td></tr>'
        return html

    def once_without_click(self, html):
        result_one, result_two = DayOnceClickRate().feed_handle()
        for key, value in result_one.items():
            if key != self.key:
                exposure, click, user, rate = value[0], value[1], result_two[key], value[2]
                feed_titile = '<tr><td rowspan="6">过滤一次未点击</td><td></td><td>曝光量</td><td>点击量</td><td>去重用户</td><td>点击率</td></tr>'
                html += feed_titile
                for key in ['ig_new', 'ig_hot', 'ir', 'tsg']:
                    if exposure[key] == 0:
                        html += '<tr><td>' + key + '</td><td>' + '0' + '</td><td>' + '0' + '</td><td>' + '0' + '</td><td>' + '0.00%' + '</td></tr>'
                    else:
                        html += '<tr><td>' + key + '</td><td>' + str(exposure[key]) + '</td><td>' + str(
                            click[key]) + '</td><td>' + str(user[key]) + '</td><td>' + rate[key] + '</td></tr>'
                exprose_sum = sum(exposure.itervalues())
                click_sum = sum(click.itervalues())
                if float(exprose_sum) != float(0):
                    html += '<tr><td>总计</td><td>' + str(exprose_sum) + '</td><td>' + str(click_sum) + '</td><td>' + str(
                       sum([v for v in user.values()])) + '</td><td>' + '{:.2%}'.format(
                       float(click_sum) / float(exprose_sum)) + '</td></tr>'
                else:
                    html += '<tr><td>总计</td><td>' + str(exprose_sum) + '</td><td>' + str(click_sum) + '</td><td>' + str(
                       sum([v for v in user.values()])) + '</td><td>' + '{:.2%}'.format(
                       float(0)) + '</td></tr>'
        return html

    def four_source(self, html):
        result_one, result_two = DaySourceClickRate().feed_handle()
        for key, value in result_one.items():
            if key != self.key:
                exposure, click, user, rate = value[0], value[1], result_two[key], value[2]
                feed_titile = '<tr><td rowspan="6">存在四种来源</td><td></td><td>曝光量</td><td>点击量</td><td>去重用户</td><td>点击率</td></tr>'
                html += feed_titile
                for key in ['ig_new', 'ig_hot', 'ir', 'tsg']:
                    if exposure[key] == 0:
                        html += '<tr><td>' + key + '</td><td>' + '0' + '</td><td>' + '0' + '</td><td>' + '0' + '</td><td>' + '0.00%' + '</td></tr>'
                    else:
                        html += '<tr><td>' + key + '</td><td>' + str(exposure[key]) + '</td><td>' + str(
                            click[key]) + '</td><td>' + str(user[key]) + '</td><td>' + rate[key] + '</td></tr>'
                exprose_sum = sum(exposure.itervalues())
                click_sum = sum(click.itervalues())
                if float(exprose_sum) != float(0):
                    html += '<tr><td>总计</td><td>' + str(exprose_sum) + '</td><td>' + str(click_sum) + '</td><td>' + str(
                       sum([v for v in user.values()])) + '</td><td>' + '{:.2%}'.format(
                       float(click_sum) / float(exprose_sum)) + '</td></tr>'
                else:
                    html += '<tr><td>总计</td><td>' + str(exprose_sum) + '</td><td>' + str(click_sum) + '</td><td>' + str(
                       sum([v for v in user.values()])) + '</td><td>' + '{:.2%}'.format(
                       float(0)) + '</td></tr>'
        return html

    def once_click_and_four_source(self, html):
        result_one, result_two = DaySourceOnceClickRate().feed_handle()
        for key, value in result_one.items():
            if key != self.key:
                exposure, click, user, rate = value[0], value[1], result_two[key], value[2]
                feed_titile = '<tr><td rowspan="6">过滤一次未点<br/>击用户曝光且<br/>存在四种来源</td><td></td><td>曝光量</td><td>点击量</td><td>去重用户</td><td>点击率</td></tr>'
                html += feed_titile
                for key in ['ig_new', 'ig_hot', 'ir', 'tsg']:
                    if exposure[key] == 0:
                        html += '<tr><td>' + key + '</td><td>' + '0' + '</td><td>' + '0' + '</td><td>' + '0' + '</td><td>' + '0.00%' + '</td></tr>'
                    else:
                        html += '<tr><td>' + key + '</td><td>' + str(exposure[key]) + '</td><td>' + str(
                            click[key]) + '</td><td>' + str(user[key]) + '</td><td>' + rate[key] + '</td></tr>'
                exprose_sum = sum(exposure.itervalues())
                click_sum = sum(click.itervalues())
                if float(exprose_sum) != float(0):
                    html += '<tr><td>总计</td><td>' + str(exprose_sum) + '</td><td>' + str(click_sum) + '</td><td>' + str(
                        sum([v for v in user.values()])) + '</td><td>' + '{:.2%}'.format(
                        float(click_sum) / float(exprose_sum)) + '</td></tr>'
                else:
                    html += '<tr><td>总计</td><td>' + str(exprose_sum) + '</td><td>' + str(click_sum) + '</td><td>' + str(
                        sum([v for v in user.values()])) + '</td><td>' + '{:.2%}'.format(
                        float(0)) + '</td></tr>'
        return html

    def register_user_click_count(self, html):
        result_one, result_two = DayRegisterClickRate().feed_handle()
        register_click, on_register_click = DayRegisterClickRate().feed_handle()
        for key, value in result_one.items():
            if key != self.key:
                register_click, on_register_click = value, on_register_click[key]
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

    def send_msg(self):
        head_title = '<tr><td colspan="6" align="center" bgcolor="#FF0000">热点-精选频道-推荐效果数据统计</td></tr>'
        html = '<table border="1" >' + head_title
        html = self.total_count(html)
        html = self.once_without_click(html)
        html = self.four_source(html)
        html = self.once_click_and_four_source(html)
        html = self.register_user_click_count(html)
        html += '</table>'
        self.http_post(html)


if __name__ == '__main__':
    send_mail = sendMail()
    send_mail.send_msg()
