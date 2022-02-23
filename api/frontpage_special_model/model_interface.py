# -*- coding: utf-8 -*-
import pymysql
from datetime import datetime,timedelta

class load_model():
    def __init__(self):
        self.model_name = 'frontpage_special_model'
        self.model_version = '1.0'
        
    def process(self, input_json):
        conn,cursor = self.connect_to_database()
        special_info = self.get_special_info(cursor)
        
        output_dict = dict()
        output_dict['special_info'] = special_info
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
    

    def get_special_info(self,cursor):
        special_basic_info_sql = '''
        select special_id,pic_url,abstract_title from melonfield.special_info order by mark_time desc limit 5;
        '''
        cursor.execute(special_basic_info_sql)
        result = cursor.fetchall()
        special_info = [dict(zip(('special_id','pic_url','abstract_title'),x)) for x in result]
        
        special_event_info_sql = '''
        select i.event_id,i.abstract_title from melonfield.event_latest_info i join melonfield.special_related_event e on i.event_id=e.event_id where special_id=%s order by i.ts_in_min desc limit 5
        '''
        special_news_info_sql = '''
        SELECT DISTINCT
            (nb.content_id), title, DATE_FORMAT(nb.publish_time,'%%Y-%%m-%%d %%H:%%i:%%S')
        FROM
            streaming.news_basics nb
                JOIN
            melonfield.event_related_news ern ON nb.content_id = ern.content_id
                JOIN
            melonfield.special_related_event sre ON ern.event_id = sre.event_id
        WHERE
            sre.special_id = %s order by nb.publish_time desc limit 5;
        '''
        for i in special_info:
            special_id = i['special_id']
            cursor.execute(special_event_info_sql,(special_id,))
            result = cursor.fetchall()
            special_event_info = [dict(zip(('event_id','abstract_title'),x)) for x in result]
            i['special_event_info'] = special_event_info
            cursor.execute(special_news_info_sql,(special_id,))
            result = cursor.fetchall()
            special_news_info = [dict(zip(('content_id','title','publish_time'),x)) for x in result]
            i['special_news_info'] = special_news_info

        return special_info       
        
        
        
        
        