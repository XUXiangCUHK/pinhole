# -*- coding: utf-8 -*-
"""
Created on Tue Nov 10 14:34:28 2020

@author: user
"""

import unicodecsv
import re

class display_filter_module():
    def __init__(self):
        self.source_info = dict()
        with open('models/mainland_media.csv','rb') as f:
            info = unicodecsv.reader(f)
            next(info)
            for row in info:
                source_id = int(row[2])
                self.source_info[source_id] = dict()
                self.source_info[source_id]['source_name'] = row[1]
                self.source_info[source_id]['rank'] = int(row[7])
        with open('models/party_media.csv','rb') as f:
            info = unicodecsv.reader(f)
            next(info)
            for row in info:
                source_id = int(row[2])
                self.source_info[source_id] = dict()
                self.source_info[source_id]['source_name'] = row[1]
                self.source_info[source_id]['rank'] = int(row[6])
        with open('models/oversea_media.csv','rb') as f:
            info = unicodecsv.reader(f)
            next(info)
            for row in info:
                source_id = int(row[2])
                self.source_info[source_id] = dict()
                self.source_info[source_id]['source_name'] = row[1]
                self.source_info[source_id]['rank'] = -1

    def check_valid_display_news(self, check_news, news_pool, event_dict):
        valid_news = list()
        title_re = re.compile(r'每经[0-9早午晚]{1,2}[点报]|早财经|.{2}[早午晚]报 {0,1}[\|｜]|[\|｜丨]')
        direction_guide = event_dict.get('direction_guide',list())
        direction_guide_str = '|'.join(direction_guide)
        direction_guide_re = re.compile(direction_guide_str)
        for news_id in check_news:
            title = news_pool[news_id]['title']
            source_id = news_pool[news_id]['source_id']
            platform = news_pool[news_id].get('platform','no')
            if not re.search(title_re,title) and direction_guide_re.search(title) and self.source_info.get(source_id)!=-1 and platform != 'tush':
                valid_news.append(news_id)
        return valid_news
    
