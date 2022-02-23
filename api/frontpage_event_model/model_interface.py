# -*- coding: utf-8 -*-
import pymysql
from datetime import datetime,timedelta

class load_model():
    def __init__(self):
        self.model_name = 'frontpage_event_model'
        self.model_version = '1.0'
        
    def process(self, input_json):
        rank_type = input_json.get('rank_type', 'latest')
        conn,cursor = self.connect_to_database()
        
        event_list = self.get_event_info(cursor,rank_type)
        
        output_dict = dict()
        output_dict['event_list'] = event_list
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
    

    def get_event_info(self,cursor,rank_type):
        today = datetime.now() - timedelta(days=5)
        ts_in_min = int(str(today.year) + str(today.month).zfill(2) + str(today.day).zfill(2) + str(today.hour).zfill(2) + str(today.minute).zfill(2))
        if rank_type == 'hotness':
            event_list_sql = '''
            select eli.event_id,ie.pic_url,ie.abstract_title from melonfield.event_latest_info eli join melonfield.important_event ie on eli.event_id = ie.event_id where ts_in_min>=%s order by news_num desc limit 50;
            '''
        elif rank_type == 'latest':
            event_list_sql = '''
            select eli.event_id,ie.pic_url,ie.abstract_title from melonfield.event_latest_info eli join melonfield.important_event ie on eli.event_id = ie.event_id where ts_in_min>=%s order by ts_in_min desc limit 50;
            '''
        else:
            return []
        cursor.execute(event_list_sql,(ts_in_min,))
        result = cursor.fetchall()
        event_list = [dict(zip(('event','pic_url','abstract_title'),x)) for x in result]

        return event_list       
        
        
        
        
        