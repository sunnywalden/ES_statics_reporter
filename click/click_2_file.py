# -*- coding:utf-8 -*-

import time, os


class ClickCalc:
    def __init__(self):
        self.timestamp = time.localtime(time.time() - 24 * 60 * 60)
        self.click_path = '/data/page_source/click/' + time.strftime('%Y/%m/%d/', self.timestamp)
        self.output_path = '/mnt/click/' + time.strftime('%Y/%m/', self.timestamp)
        self.day = time.strftime('%d', self.timestamp)
        self.bigdata_click_path = '/mnt/bigdata_click/click_data/' + time.strftime('%Y%m%d/', self.timestamp)
        self.bigdata_file_name = time.strftime('%Y%m%d', self.timestamp) + '_click_data_00_001.dat'
        self.click_num_rate = 0.6

    def same_bigdata(self):
        bigdata_dict = {}
        with open(self.bigdata_click_path + self.bigdata_file_name, 'r') as fs:
            for f in fs:
                line = f.split('|')
                num = int(line[1].strip())
                click_num = int(num * self.click_num_rate) if num > 10 else num
                bigdata_dict[line[0]] = click_num
        return bigdata_dict

    def bigdata_diff_data(self):
        result_dict = {}
        click_data = self.read_click_data()
        bigdata_dict = self.same_bigdata()
        diff = set(bigdata_dict.keys()) - set(click_data.keys())
        for d in diff:
            result_dict[d] = bigdata_dict[d]
        return result_dict

    def read_click_data(self):
        search_id_dict = {}
        file_list = os.listdir(self.click_path)
        for file in file_list:
            with open(self.click_path + file, 'r') as fs:
                for f in fs:
                    line = f.strip().split('|')
                    search_id = line[4][0:9]
                    if search_id in search_id_dict:
                        search_id_dict[search_id] += 1
                    else:
                        search_id_dict[search_id] = 1
        return search_id_dict

    def write_click_data(self):
        diff = self.bigdata_diff_data()
        lines = dict(self.read_click_data(), **diff)
        if not os.path.exists(self.output_path):
            os.makedirs(self.output_path)
        with open(self.output_path + self.day, 'a') as f:
            for key, value in lines.items():
                f.write(str(key) + ':' + str(value) + '\n')


if __name__ == '__main__':
    print 'start handle ......'
    click_calc = ClickCalc()
    click_calc.write_click_data()
