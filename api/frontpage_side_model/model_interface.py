# -*- coding: utf-8 -*-
import pymysql
from datetime import datetime,timedelta

class load_model():
    def __init__(self):
        self.model_name = 'frontpage_side_model'
        self.model_version = '1.0'
        
    def process(self, input_json):
        conn,cursor = self.connect_to_database()
        
        user_like = self.get_user_like(cursor)
        latest_express = self.get_latest_express(cursor)
        hot_news = self.get_hot_news(cursor)
        recommand_event = self.get_recommand_event(cursor)
        
        output_dict = dict()
        output_dict['user_like'] = user_like
        output_dict['latest_express'] = latest_express
        output_dict['hot_news'] = hot_news
        output_dict['recommand_event'] = recommand_event
        
        cursor.close()
        conn.close()
        return output_dict

    def get_version(self):
        return self.model_version

    def get_name(self):
        return self.model_name

    def connect_to_database(self):
        conn = pymysql.connect(
        host='rm-wz9lh12zwnbo4b457.mysql.rds.aliyuncs.com',port=3306,
        user='melonfield',password='melonfield@DG_2020',
        charset='utf8mb4')
        cursor = conn.cursor()
        return conn,cursor
    
    def get_user_like(self,cursor):
        user_like_sql = '''
        select event_id,pic_url,abstract_title from melonfield.important_event order by mark_time desc limit 10,50;
        '''
        cursor.execute(user_like_sql)
        result = cursor.fetchall()
        user_like = [dict(zip(('event_id','pic_url','abstract_title'),x)) for x in result]
        return user_like
        
    def get_latest_express(self,cursor):
        latest_express_event_sql = '''
        select content_id,DATE_FORMAT(publish_time,"%Y-%m-%d %H:%i:%S"),title from melonfield.important_news order by publish_time desc limit 10;
        '''
        cursor.execute(latest_express_event_sql)
        result = cursor.fetchall()
        latest_express = [dict(zip(('content_id','publish_time','title'),x)) for x in result]
        return latest_express
    
    def get_hot_news(self,cursor):
        hot_news_sql = '''
        select content_id,abstract_title from melonfield.recent_hot_news order by hotness desc limit 50;
        '''
        cursor.execute(hot_news_sql)
        result = cursor.fetchall()
        hot_news = [dict(zip(('content_id','abstract_title'),x)) for x in result]
        return hot_news       
        
    def get_recommand_event(self,cursor):
        recommand_event_sql = '''
        select event_id,pic_url,abstract_title from melonfield.important_event order by mark_time desc limit 10,50;
        '''
        cursor.execute(recommand_event_sql)
        result = cursor.fetchall()
        recommand_event = [dict(zip(('event_id','pic_url','abstract_title'),x)) for x in result]
        return recommand_event        
        
