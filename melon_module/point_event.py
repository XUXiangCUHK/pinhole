# -*- coding: utf-8 -*-

from datetime import datetime, timedelta
from models.database import Database
from models.event_detection import Event_detection
from models.news_pool_V2 import News_pool_tag
from models.tag import Tag
from models.update import Update
import requests

class Point_event:
    def __init__(self):
        self.database = Database()
        self.event_detection = Event_detection()
        self.key_mode = 'and'
        self.db_command = '''
                       SELECT f.content_id 
                       FROM melonfield.filtered_news_info f
                       JOIN streaming.news_basics b on f.content_id = b.content_id
                       WHERE ({}) AND (b.publish_time >= '{}' and b.publish_time < '{}') 
                       ORDER BY b.publish_time ASC;
                       '''

    def make_db_command(self, key_words_list, interval):
        int_first = interval.split()[0].strip()
        int_second = interval.split()[1].strip()
        key_list = [" b.title like '%{}%' ".format(key) for key in key_words_list]
        title_seg = self.key_mode.join(key_list)
        full_command = self.db_command.format(title_seg, int_first, int_second)
        # print(full_command)
        return full_command

    def find_all_related_news(self, key_words_list, interval):
        content_id_list = list()
        full_command = self.make_db_command(key_words_list, interval)
        select_query_result = self.database.read_from_mysql(full_command)
        for item in select_query_result:
            content_id_list.append(item[0])
        
        news_cluster_pool = {
            1: {
                'event_count': 0,
                'repost_num': len(content_id_list),
                'news_collection': content_id_list
            }
        }
        ts_in_min = datetime.now().strftime('%Y%m%d%H%M')
        event_id = self.event_detection.find_new_event(news_cluster_pool, ts_in_min, max_news_num=1500)
        return event_id
    
    def ES_search(self, keywords, operator, minimum_should_match, only_title, start_time, end_time):
        input_dict = {
                'keywords': keywords,
                'operator': operator,
                'page_size': 10000,
                'minimum_should_match': minimum_should_match,
                'only_title': only_title,
                'source': 'news',
                'start_time': start_time,
                'end_time': end_time
        }
        url = 'http://172.18.223.14:8903/search_news_ids'
        print(input_dict)
        response = requests.post(url=url, json=input_dict)
        ES_search_id_list = response.json()['ids']
        print('Finish ES search')

        stored_news_id = list()
        news_sql = '''
                SELECT f.content_id 
                FROM melonfield.filtered_news_info f
                JOIN streaming.news_basics b on f.content_id = b.content_id
                WHERE b.publish_time >='{}' AND b.publish_time < '{}'
                ORDER BY b.publish_time ASC;
                '''.format(start_time, end_time)
        select_query_result = self.database.read_from_mysql(news_sql)
        for item in select_query_result:
            stored_news_id.append(item[0])
        print('Finish mysql fetch')
        
        content_id_list = list(set(ES_search_id_list).intersection(set(stored_news_id)))
        news_cluster_pool = {
            1: {
                'event_count': 0,
                'repost_num': len(content_id_list),
                'news_collection': content_id_list
            }
        }
        ts_in_min = datetime.now().strftime('%Y%m%d%H%M')
        event_id = self.event_detection.find_new_event(news_cluster_pool, ts_in_min, max_news_num=10000)
        return event_id

    def mimic_tag_process(self, event_id, tag_from_time):
        time_now = datetime.strptime(tag_from_time[:10], '%Y-%m-%d')
        while True:
            # confirm time
            ts_in_min = time_now.strftime('%Y%m%d%H%M')
            print(ts_in_min)
            start_time_str, end_time_str = self.database.get_time_interval(time_now, gap=12*60)

            # create news_pool waiting for tag
            NPool_tag = News_pool_tag(start_time_str, end_time_str)
            news_pool_tag = NPool_tag.create_news_pool_tag(gap=12*60)
            # create existing event_pool
            event_pool = self.database.get_event_pool_by_id(event_id)

            # tag news
            TAG = Tag(min_overlap=7, ts_in_min=ts_in_min)
            TAG.tag_input_news_iterative(news_pool_tag, event_pool)

            # make timeline
            # ZERO = NewsTypeZero()
            # ZERO.auto_timeline(update_event_id_list, start_time_str, ts_in_min=ts_in_min)

            # update event
            UPDATE = Update()
            UPDATE.update_event_by_id([event_id], ts_in_min)

            print('Finish!')
            # update time
            time_now = time_now + timedelta(minutes = 12*60)
            if time_now > datetime.now():
                break