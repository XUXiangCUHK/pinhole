# -*- coding: utf-8 -*-
from datetime import datetime,timedelta
import requests
from database_related.event_data_manipulator import event_data_manipulator
from database_related.news_data_manipulator import news_data_manipulator
from melon_module.leaf import Leaf
from collections import Counter

class load_model():
    def __init__(self):
        self.model_name = 'compare_timeline_model'
        self.model_version = '1.0'
        self.event_dm = event_data_manipulator()
        self.news_dm = news_data_manipulator()
        self.leaf = Leaf()
        
    def process(self, input_json):
        event_id = input_json.get('event_id')
        cut_time = input_json.get('cut_time')
        cut_time = datetime.strptime(cut_time,'%Y-%m-%d')
        
        event_news_pool = self.news_dm.get_event_news_info(event_id)
        news_collection = list()
        news_pool = dict()
        for key,value in event_news_pool.items():
            if value['publish_time'] <= cut_time:
                news_collection.append(key)
                news_pool[key] = value
        
        agglo_timeline = self.leaf.make_leaf_for_news_collection(news_collection, news_pool, max_num=10)
        
        arena_timeline = self.event_dm.get_event_history_timeline(event_id, cut_time)

        for key,value in news_pool.items():
            news_pool[key]['publish_time'] = news_pool[key]['publish_time'].strftime('%Y-%m-%d %H:%M:%S')
        
        output_dict = dict()
        output_dict['agglo_timeline'] = agglo_timeline
        output_dict['news_pool'] = news_pool
        output_dict['arena_timeline'] = arena_timeline
        return output_dict

    def get_version(self):
        return self.model_version

    def get_name(self):
        return self.model_name

    def svc(self,url, input_dict):
        response = requests.post(url=url, json=input_dict)
        return response.json() if response.ok else False

