# -*- coding: utf-8 -*
import json
import socket
import urllib2
from datetime import date, timedelta

from total_day import DayClickRate
from total_once_day import DayOnceClickRate
from total_sources_day import DaySourceClickRate
from total_sources_once_day import DaySourceOnceClickRate
from activity_user_click_count import ActivityUserClickCount
from total_register_day import DayRegisterClickRate
from register_user_count import RegisterUserCount 
#from activity_user_click_count import ActivityUserClickCount
from subchannel_count import SubchannelCount

class sendMail:
    def __init__(self):
        #self.day = (date.today() - timedelta(1)).strftime("%Y年%m月%d日")
        self.day = (date.today() - timedelta(1)).strftime("%Y年%m月%d日")
        self.day_before_yesterday = (date.today() - timedelta(1)).strftime("%Y-%m-%d") + 'T00:00:00.000+08:00'
        self.key = (date.today() - timedelta(2)).strftime("%Y-%m-%d") + 'T00:00:00.000+08:00'
        #t = date(2018,9,1)
        #self.day = (t - timedelta(1)).strftime("%Y年%m月%d日")
        #self.day_before_yesterday = (t - timedelta(1)).strftime("%Y-%m-%d") + 'T00:00:00.000+08:00'
        #self.key = (t - timedelta(2)).strftime("%Y-%m-%d") + 'T00:00:00.000+08:00'
        self.sender = "no-reply-devops@cloudin.cn"  # 发件人邮箱
        self.password = "migu1qaz!QAZ"  # 发件人密码
        self.subject = "统计结果：%s-feed和子频道统计" % self.day  # 邮件标题
        self.mail_server_url = 'http://10.200.26.14:5000/sendmail'
        self.receivers = ['shanghai@cloudin.cn', 'sangyongjia@migu.cn', 'wangdongguang@migu.cn']
        #self.receivers = ['zhangbo@cloudin.cn','zhoubin@cloudin.cn']
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
        if len(y) != 0: 
            return '%.2f' % (float(x) / float(len(y)))
        else:
            return 0.0

    def total_count(self, html):
        result_one, result_two, result_three = DayClickRate().feed_handle()
        for key, value in result_one.items():
            if key != self.key:
                #exposure, click, user, rate = value[0], value[1], result_two[key], value[2]
                exposure, exusers, click, clusers, avgclick, rate = value[0], value[1], value[2], value[3], value[4], value[5]
                exusers_sum = result_three[key]['exposure']
                clusers_sum = result_three[key]['click']
                feed_titile = '<tr><td rowspan="6">总体</td><td></td><td>曝光量</td><td>曝光用户</td><td>点击量</td><td>点击用户</td><td>平均点击次数</td><td>点击率</td></tr>'
                html += feed_titile
                for key in ['ig_new', 'ig_hot', 'ir', 'tsg']:
                    if exposure[key] == 0:
                        html += '<tr><td>' + key + '</td><td>' + '0' + '</td><td>' + '0' + '</td><td>' + '0'  + '</td><td>' + '0' + '</td><td>' + '0' + '</td><td>' + '0.00%' + '</td></tr>'
                    else:
                        html += '<tr><td>' + key + '</td><td>' + str(exposure[key]) + '</td><td>' + str(exusers[key])  + '</td><td>' + str(click[key]) + '</td><td>' + str(clusers[key]) + '</td><td>' + str(avgclick[key]) + '</td><td>' + rate[key] + '</td></tr>'
                exprose_sum = sum(exposure.itervalues())
                #exusers_sum = sum(exusers.itervalues())

                click_sum = sum(click.itervalues())
                #clusers_sum = sum(clusers.itervalues())

                if clusers_sum != 0:
                    avgclick_sum = round(float(click_sum) / float(clusers_sum), 1)
                else:
                    avgclick_sum = 0.0
                html += '<tr><td>总计</td><td>' + str(exprose_sum) + '</td><td>'+ str(exusers_sum) + '</td><td>'  + str(click_sum) + '</td><td>' + str(clusers_sum) + '</td><td>' + str(avgclick_sum) + '</td><td>' + '{:.2%}'.format(
                    float(click_sum) / float(exprose_sum)) + '</td></tr>'
        return html

    def once_without_click(self, html):
        result_one, result_two, result_three = DayOnceClickRate().feed_handle()
        for key, value in result_one.items():
            if key != self.key:
                exposure, exusers, click, clusers, avg_click, rate = value[0], value[1], value[2], value[3], value[4], value[5]
                exusers_sum = result_three[key]['exposure']
                clusers_sum = result_three[key]['click']
                feed_titile = '<tr><td rowspan="6">过滤一次未点击</td><td></td><td>曝光量</td><td>曝光用户</td><td>点击量</td><td>点击用户</td><td>平均点击次数</td><td>点击率</td></tr>'
                html += feed_titile
                for key in ['ig_new', 'ig_hot', 'ir', 'tsg']:
                    if exposure[key] == 0:
                        html += '<tr><td>' + key + '</td><td>' + '0' + '</td><td>' + '0' + '</td><td>' + '0'  + '</td><td>' + '0' + '</td><td>' + '0' + '</td><td>' + '0.00%' + '</td></tr>'
                    else:
                        html += '<tr><td>' + key + '</td><td>' + str(exposure[key]) + '</td><td>' + str(exusers[key])  + '</td><td>' + str(
                            click[key]) + '</td><td>' + str(clusers[key]) + '</td><td>' + str(avg_click[key]) + '</td><td>' + rate[key] + '</td></tr>'
                exprose_sum = sum(exposure.itervalues())
                #exusers_sum = sum(exusers.itervalues())
                click_sum = sum(click.itervalues())
                #clusers_sum = sum(clusers.itervalues())
                if clusers_sum != 0:
                    avg_click_sum = round(float(click_sum) / float(clusers_sum), 1)
                else:
                    avg_click_sum = 0
                html += '<tr><td>总计</td><td>' + str(exprose_sum) + '</td><td>' + str(exusers_sum)  + '</td><td>' + str(click_sum) + '</td><td>' + str(
                    clusers_sum) + '</td><td>' + str(avg_click_sum)  + '</td><td>' + '{:.2%}'.format(
                    float(click_sum) / float(exprose_sum)) + '</td></tr>'
        return html

    def four_source(self, html):
        result_one, result_two, result_three = DaySourceClickRate().feed_handle()
        for key, value in result_one.items():
            if key != self.key:
                exposure, exusers, click, clusers, avgclick, rate = value[0], value[1], value[2], value[3], value[4], value[5]
                exusers_sum = result_three[key]['exposure']
                clusers_sum = result_three[key]['click']
                feed_titile = '<tr><td rowspan="6">存在四种来源</td><td></td><td>曝光量</td><td>曝光用户</td><td>点击量</td><td>点击用户</td><td>平均点击次数</td><td>点击率</td></tr>'
                html += feed_titile
                for key in ['ig_new', 'ig_hot', 'ir', 'tsg']:
                    if exposure[key] == 0:
                        html += '<tr><td>' + key + '</td><td>' + '0' + '</td><td>' + '0' + '</td><td>' + '0' + '</td><td>' + '0' + '</td><td>' + '0' + '</td><td>' + '0.00%' + '</td></tr>'
                    else:
                        html += '<tr><td>' + key + '</td><td>' + str(exposure[key]) + '</td><td>' + str(exusers[key])  + '</td><td>' + str(
                            click[key]) + '</td><td>' + str(clusers[key]) + '</td><td>' + str(avgclick[key]) + '</td><td>' + rate[key] + '</td></tr>'
                exprose_sum = sum(exposure.itervalues())
                #exusers_sum = sum(exusers.itervalues())
                click_sum = sum(click.itervalues())
                #clusers_sum = sum(clusers.itervalues())
                if clusers_sum != 0:
                    avgclick_sum = round(float(click_sum) / float(clusers_sum), 1)
                else:
                    avgclick_sum = 0.0
                if float(exprose_sum) != float(0):
                    html += '<tr><td>总计</td><td>' + str(exprose_sum) + '</td><td>' + str(exusers_sum) + '</td><td>'  + str(click_sum) + '</td><td>' + str(
                        clusers_sum) + '</td><td>' + str(avgclick_sum) + '</td><td>' + '{:.2%}'.format(
                        float(click_sum) / float(exprose_sum)) + '</td></tr>'
                else:
                    html += '<tr><td>总计</td><td>' + str(exprose_sum) + '</td><td>' + str(exusers_sum) + '</td><td>'  + str(click_sum) + '</td><td>' + str(
                        clusers_sum) + '</td><td>' + str(avgclick_sum) + '</td><td>' + '{:.2%}'.format(
                        float(0)) + '</td></tr>'
                    
        return html

    def once_click_and_four_source(self, html):
        result_one, result_two, result_three = DaySourceOnceClickRate().feed_handle()
        for key, value in result_one.items():
            if key != self.key:
                exposure, exusers, click, clusers, avgclick, rate = value[0], value[1], value[2], value[3], value[4], value[5]
                exusers_sum = result_three[key]['exposure']
                clusers_sum = result_three[key]['click']
                feed_titile = '<tr><td rowspan="6">过滤一次未点<br/>击用户曝光且<br/>存在四种来源</td><td></td><td>曝光量 \
                    </td><td>曝光用户</td><td>点击量</td><td>点击用户</td><td>平均点击次数</td><td>点击率</td></tr>'
                html += feed_titile
                for key in ['ig_new', 'ig_hot', 'ir', 'tsg']:
                    if exposure[key] == 0:
                        html += '<tr><td>' + key + '</td><td>' + '0' + '</td><td>' + '0' + '</td><td>' + '0' + '</td><td>' + '0' + '</td><td>' + '0' + '</td><td>' \
                                + '0.00%' + '</td></tr>'
                    else:
                        html += '<tr><td>' + key + '</td><td>' + str(exposure[key]) + '</td><td>' + str(exusers[key]) + '</td><td>' + str(
                            click[key]) + '</td><td>' + str(clusers[key]) + '</td><td>' + str(avgclick[key]) + '</td><td>' + rate[key] + '</td></tr>'
                exprose_sum = sum(exposure.itervalues())
                #exusers_sum = sum(exusers.itervalues())
                click_sum = sum(click.itervalues())
                #clusers_sum = sum(clusers.itervalues())
                if clusers_sum != 0:
                    avgclick_sum = round(float(click_sum) / float(clusers_sum), 1)
                if float(exprose_sum) != float(0):
                    html += '<tr><td>总计</td><td>' + str(exprose_sum) + '</td><td>' + str(exusers_sum) + '</td><td>' + str(click_sum) + '</td><td>' + str(
                        clusers_sum) + '</td><td>' + str(avgclick_sum) + '</td><td>' + '{:.2%}'.format(
                        float(click_sum) / float(exprose_sum)) + '</td></tr>'
                else:
                    html += '<tr><td>总计</td><td>' + str(exprose_sum) + '</td><td>' + str(exusers_sum) + '</td><td>' + str(click_sum) + '</td><td>' + str(
                        clusers_sum) + '</td><td>' + str(avgclick_sum) + '</td><td>' + '{:.2%}'.format(
                        float(0)) + '</td></tr>'
        return html

    def activity_user_click(self, html):
        click, user, average_click = ActivityUserClickCount().feed_handle()
        user_title = '<tr><td rowspan="2">活跃用户-点击</td><td></td><td>活跃用户点击量</td><td>活跃用户量 \
		</td><td>平均点击次数</td><td align="center">-</td><td align="center">-</td><td align="center">-</td></tr>'
        html += user_title
        html += '<tr><td>总计</td><td>' + str(click) + '</td><td>' + str(user) + '</td><td>' + str(average_click) + \
		'</td><td align="center">-</td><td align="center">-</td><td align="center">-</td></tr>'
        return html

    def register_user_count(self, html):
    #def register_user_count(self, request, html):
        #active_tuple, register_tuple, no_register_tuple = request
        active_tuple, register_tuple, no_register_tuple = RegisterUserCount().feed_handle()
        day_b_yes = self.day_before_yesterday
        active = active_tuple[day_b_yes]
        register = register_tuple[day_b_yes]
        no_register = no_register_tuple[day_b_yes]
        print('debug register_user_count',active, register, no_register)
        active_title = '<tr><td rowspan="2">活跃用户-请求</td><td></td><td>活跃用户请求量</td><td>活跃用户量</td> \
		<td>平均请求次数</td><td align="center">-</td><td align="center">-</td><td align="center">-</td></tr>'
        registered_title = '<tr><td rowspan="2">注册</td><td></td><td>注册用户请求量</td><td>活跃用户量</td><td>平均请求次数 \
            </td><td align="center">-</td><td align="center">-</td><td align="center">-</td></tr>'
        no_registered_title = '<tr><td rowspan="2">未注册</td><td></td><td>未注册用户请求量</td><td>活跃用户量</td> \
		<td>平均请求次数</td><td align="center">-</td><td align="center">-</td><td align="center">-</td></tr>'
        html += active_title
        html += '<tr><td>总计</td><td>' + str(active[0]) + '</td><td>' + str(active[1]) + '</td><td>' + \
                str(active[2]) + '</td><td align="center">-</td><td align="center">-</td><td align="center">-</td></tr>'
        html += registered_title
        html += '<tr><td>总计</td><td>' + str(register[0]) + '</td><td>' + str(register[1]) + \
		'</td><td>' + str(register[2]) + '</td><td align="center">-</td><td align="center">-</td><td align="center">-</td></tr>'
        html += no_registered_title
        html += '<tr><td>总计</td><td>' + str(no_register[0]) + '</td><td>' + str(no_register[1]) + \
		'</td><td>' + str(no_register[2]) + '</td><td align="center">-</td><td align="center">-</td><td align="center">-</td></tr>'
        return html


    def register_user_click_count(self, html):
        result_one, result_two = DayRegisterClickRate().feed_handle()
        register_click, on_register_click = DayRegisterClickRate().feed_handle()
        for key, value in result_one.items():
            if key != self.key:
                register_click, on_register_click = value, on_register_click[key]
                registered_title = '<tr><td rowspan="6">注册<br/>区分来源</td><td></td><td>点击量</td> \
			<td align="center">-</td><td align="center">-</td><td align="center">-</td><td align="center">-</td><td align="center">-</td></tr>'
                no_registered_title = '<tr><td rowspan="6">未注册<br/>区分来源</td><td></td><td>点击量</td> \
			<td align="center">-</td><td align="center">-</td><td align="center">-</td><td align="center">-</td><td align="center">-</td></tr>'
                html += registered_title
                for key in ['ig_new', 'ig_hot', 'ir', 'tsg']:
                    if register_click.get(key):
                        register_s = register_click.get(key)
                    else:
                        register_s, register_click[key] = 0, 0
                    html += '<tr><td>' + key + '</td><td>' + str(
                        register_s) + '</td><td align="center">-</td><td align="center">-</td><td align="center">-</td><td align="center">-</td><td align="center">-</td></tr>'
                html += '<tr><td>总计</td><td>' + str(
                    reduce(lambda x, y: x + y, [register_click[k] for k in ['ig_new', 'ig_hot', 'ir','tsg']])) + \
                            '</td><td align="center">-</td><td align="center">-</td><td align="center">-</td><td align="center">-</td><td align="center">-</td></tr>'
                html += no_registered_title
                for key in ['ig_new', 'ig_hot', 'ir', 'tsg']:
                    if on_register_click.get(key):
                        no_register_s = on_register_click[key]
                    else:
                        no_register_s, on_register_click[key] = 0, 0
                    html += '<tr><td>' + key + '</td><td>' + str(
                        no_register_s) + '</td><td align="center">-</td><td align="center">-</td><td align="center">-</td><td align="center">-</td><td align="center">-</td></tr>'
                html += '<tr><td>总计</td><td>' + str(
                    reduce(lambda x, y: x + y, [on_register_click[k] for k in ['ig_new', 'ig_hot', 'ir','tsg']])) + \
                        '</td><td align="center">-</td><td align="center">-</td><td align="center">-</td><td align="center">-</td><td align="center">-</td></tr>'
        return html

    def subchannel_count(self, html):
        #result = SubchannelCount().subchannel()
        result = SubchannelCount().feed_handle()
        sub_head_titile = '<tr><td colspan="8" align="center" bgcolor="#FF0000">子频道-数据统计</td></tr>'
        sub_titile = '<tr><td rowspan="15">子频道</td><td></td><td>点击量</td><td>去重用户</td><td align="center">-</td> \
            <td align="center">-</td><td align="center">-</td><td align="center">-</td></tr>'
        html += sub_head_titile
        html += sub_titile
        for v in result:
        #for k, v in result.items():
            #value = v.split('|')
            value = v
            html += '<tr><td>' + str(value[1]) + '</td><td>' + str(value[2]) + '</td><td>' + \
		str(value[3]) + '</td><td align="center">-</td><td align="center">-</td><td align="center">-</td><td align="center">-</td></tr>'
            #html += '<tr><td>' + k + '</td><td>' + str(value[0]) + '</td><td>' + str(
            #    value[1]) + '</td><td align="center">-</td><td align="center">-</td></tr>'
        html += '<tr><td>总计</td><td>' + str(reduce(lambda x, y: x + y,[v[2] for v in result])) + \
		'</td><td>' + str(reduce(lambda x, y: x + y,[v[3] for v in result])) +  \
		'</td><td align="center">-</td><td align="center">-</td><td align="center">-</td><td align="center">-</td></tr>'
        #html += '<tr><td>总计</td><td>' + str(
        #    sum([int(v.split('|')[0]) for v in result.values()])) + '</td><td>' + str(
        #    sum([int(v.split('|')[1]) for v in
        #         result.values()])) + '</td><td align="center">-</td><td align="center">-</td></tr>'
        html += '</table>'
        return html

    def send_msg(self):
        head_title = '<tr><td colspan="8" align="center" bgcolor="#FF0000">热点-精选频道-推荐效果数据统计</td></tr>'
        html = '<table border="1" >' + head_title
        html = self.total_count(html)
        html = self.once_without_click(html)
        html = self.four_source(html)
        html = self.once_click_and_four_source(html)
        #added by henry
        html = self.activity_user_click(html)
        #added by henry
        html = self.register_user_count(html)
        html = self.register_user_click_count(html)
        #html = self.register_user_count(RegisterUserCount.feed_handle(), html)
        html = self.subchannel_count(html)
        html += '</table>'
        self.http_post(html)


if __name__ == '__main__':
    send_mail = sendMail()
    send_mail.send_msg()
