# -*- coding: utf-8 -*-
import pymysql
from datetime import datetime,timedelta

class load_model():
    def __init__(self):
        self.model_name = 'newspage_model'
        self.model_version = '1.0'
        
    def process(self, input_json):
        content_id = input_json.get('content_id', None)
        conn,cursor = self.connect_to_database()
        
        current_news = self.get_current_news(cursor,content_id)
        event_list = self.get_event_list(cursor,content_id)
        
        output_dict = dict()
        output_dict['current_news'] = current_news
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
    
    def get_current_news(self,cursor,content_id):
        current_news_sql = '''
        SELECT 
            b.source_name,
            DATE_FORMAT(r.publish_time, '%%Y-%%m-%%d %%H:%%i:%%S'),
            r.title,
            r.content
        FROM
            streaming.news_basics_raw r
                JOIN
            streaming.news_basics b
            on r.publish_id = b.content_id
        where b.content_id = %s;      
        '''
        cursor.execute(current_news_sql,(content_id,))
        result = cursor.fetchall()
        current_news = dict(zip(('source_name','publish_time','title','content'),result[0]))
        return current_news       

    def get_event_list(self,cursor,content_id):
        event_list_sql = '''
        SELECT 
            i.event_id,
            i.abstract_title
        FROM
            melonfield.event_related_news n
                JOIN
            melonfield.event_latest_info i
            on n.event_id = i.event_id
        where n.content_id = %s limit 5;      
        '''
        cursor.execute(event_list_sql,(content_id,))
        result = cursor.fetchall()
        event_list = [dict(zip(('event_id','abstract_title'),x)) for x in result]
        return event_list       
        
    
    
        
        
        
        