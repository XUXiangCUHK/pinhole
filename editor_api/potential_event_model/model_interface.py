# -*- coding: utf-8 -*-
import pymysql
from datetime import datetime,timedelta
import requests

class load_model():
    def __init__(self):
        self.model_name = 'potential_event_model'
        self.model_version = '1.0'
        
    def process(self, input_json):
        conn,cursor = self.connect_to_database()
        page = input_json.get('page')        
        event_list = self.get_event_list(conn,cursor,page)

        output_dict = dict()
        output_dict['event_list'] = event_list
        cursor.close()
        conn.close()
        return output_dict

    def get_version(self):
        return self.model_version

    def get_name(self):
        return self.model_name


    def svc(self,url, input_dict):
        response = requests.post(url=url, json=input_dict)
        return response.json() if response.ok else False

    def connect_to_database(self):
        conn = pymysql.connect(
        host='rm-wz9lh12zwnbo4b457.mysql.rds.aliyuncs.com',port=3306,
        user='melonfield',password='melonfield@DG_2020',
        charset='utf8mb4')
        cursor = conn.cursor()
        return conn,cursor
    
    def get_event_list(self,conn,cursor,page):
        event_list_sql = '''
        SELECT 
            eli.event_id,
            eli.ts_in_min,
            eli.event_top_word,
            eli.abstract_title
        FROM
            melonfield.event_latest_info eli
                LEFT JOIN
            melonfield.important_event ie ON eli.event_id = ie.event_id
        WHERE
            ISNULL(ie.event_id)
        ORDER BY ts_in_min DESC
        LIMIT %s , 10;
        '''
        start_position = max((page-1) * 10,0)
        cursor.execute(event_list_sql,(start_position,))
        result = cursor.fetchall()
        event_list = [dict(zip(('event_id','update_time','event_top_word','event_key_word'),x)) for x in result]
        return event_list
    

