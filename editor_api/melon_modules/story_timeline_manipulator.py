# -*- coding: utf-8 -*-
"""
Created on Tue Sep 22 18:00:54 2020

@author: user
"""

from datetime import datetime, timedelta
from models.story_timeline import story_timeline
from collections import defaultdict,Counter
import re
from models.abstract import abstract
from models.event_data_manipulator import event_data_manipulator
from models.news_data_manipulator import news_data_manipulator

class story_timeline_manipulator():
    def __init__(self):
        self.sentSplitRe = re.compile(r'[^?!。？！]+(?:[?!。？！]+[”]?|$)')
        self.abt = abstract()
        self.event_dm = event_data_manipulator()
        self.news_dm = news_data_manipulator()
    
    def generate_story_timeline(self, event_id, event_type, insert_history_flag = True, timeline_runtime_gap = 1440):
        if event_type == 0:
            timeline_len = 7
        else:
            timeline_len = 4
    
        break_time = datetime.now() - timedelta(hours=6)
#        event_related_news_data = self.news_dm.get_event_related_news_data_for_timeline(event_id)
        event_news_pool = self.news_dm.get_event_news_info(event_id)
        event_info = self.event_dm.get_event_info(event_id)
        history_list = list()
        updated_news = list()
        news_collection = list()
        st = story_timeline()
#        timeline_runtime_gap = 480
        flag = 0
        abstract_dict = dict()
        
        for news_id, news_info in event_news_pool.items():
            publish_time = news_info['publish_time']
            if flag == 0:
                cut_time = publish_time + timedelta(days=1)
                flag = 1
            if publish_time >= break_time:
                break
            if publish_time>=cut_time and flag==1:
                timeline = st.story_timeline_init(news_collection, event_news_pool, event_info, timeline_len) 
                history_list = timeline
                updated_news = list()
                ts_in_min = cut_time.strftime('%Y%m%d%H%M')
                while cut_time <= publish_time:
                    cut_time += timedelta(minutes=timeline_runtime_gap)  
                flag = 2
                if insert_history_flag:
                    insert_list = list()
                    for i in history_list:
                        insert_dict = dict()
                        insert_dict['event_id'] = event_id
                        insert_dict['content_id'] = i
                        insert_list.append(insert_dict)
                    self.event_dm.insert_event_timeline(insert_list, ts_in_min)
            elif publish_time>=cut_time and updated_news!=list():
                timeline = st.story_timeline_update('', history_list, updated_news, news_collection, event_news_pool, event_info, timeline_len)  
                history_list = timeline
                updated_news = list()
                ts_in_min = cut_time.strftime('%Y%m%d%H%M')
                insert_list = list()
                if insert_history_flag:
                    insert_list = list()
                    for i in history_list:
                        insert_dict = dict()
                        insert_dict['event_id'] = event_id
                        insert_dict['content_id'] = i
                        insert_list.append(insert_dict)
                    self.event_dm.insert_event_timeline(insert_list, ts_in_min)
                while cut_time <= publish_time:
                    cut_time += timedelta(minutes=timeline_runtime_gap)
            news_collection.append(news_id)
            updated_news.append(news_id)

        timeline = st.story_timeline_update('', history_list, updated_news, news_collection, event_news_pool, event_info, timeline_len)
        ts_in_min = cut_time.strftime('%Y%m%d%H%M')
        # print('----------------------update----------------------',cut_time)
        insert_list = list()
        for i in timeline:
            print(i,event_news_pool[i]['title'],event_news_pool[i]['publish_time'],ts_in_min)    
            insert_dict = dict()
            insert_dict['event_id'] = event_id
            insert_dict['content_id'] = i
            insert_list.append(insert_dict)
            if not abstract_dict.get(i):
                short_abstract = self.abt.get_news_abstract(event_news_pool[i]['title'], event_news_pool[i]['content'])
                abstract = self.abt.get_news_abstract(event_news_pool[i]['title'], event_news_pool[i]['content'], 300)
                highlight = self.abt.get_highlight_sent(abstract)
                insert_dict = dict()
                insert_dict['content_id'] = i
                insert_dict['title'] = event_news_pool[i]['title']
                insert_dict['abstract'] = abstract
                insert_dict['short_abstract'] = short_abstract
                insert_dict['highlight'] = highlight
                abstract_dict[i] = insert_dict

        abstract_list = list()
        for key,value in abstract_dict.items():
            abstract_list.append(value)

        self.event_dm.insert_event_timeline(insert_list, ts_in_min)
        self.news_dm.insert_news_abstract(abstract_list) 
        history_list = timeline
        updated_news = list()
        
    def regenerate_story_timeline(self, event_id, event_type):
#        try:
            self.event_dm.clean_event_timeline(event_id)
            self.generate_story_timeline(event_id, event_type)
            return 'success'
#        except Exception as e:
#            print(e)
#            return 'fail'
    