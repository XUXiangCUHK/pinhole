# -*- coding: utf-8 -*-
import pymysql
from datetime import datetime,timedelta

class load_model():
    def __init__(self):
        self.model_name = 'event_list_model'
        self.model_version = '1.0'
        
    def process(self, input_json):
        conn,cursor = self.connect_to_database()
        important_event = self.get_important_event(cursor)
        
        output_dict = dict()
        output_dict['important_event'] = important_event
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
    
    def get_important_event(self,cursor):
        important_event_sql = '''
        select event_id,pic_url,abstract_title from melonfield.important_event order by mark_time desc limit 50;
        '''
        cursor.execute(important_event_sql)
        result = cursor.fetchall()
        important_event = [dict(zip(('event_id','pic_url','abstract_title'),x)) for x in result]
        return important_event
        
        