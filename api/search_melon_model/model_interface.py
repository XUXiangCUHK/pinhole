# -*- coding: utf-8 -*-
import pymysql
from datetime import datetime,timedelta
import requests

class load_model():
    def __init__(self):
        self.model_name = 'search_melon_model'
        self.model_version = '1.0'
        
    def process(self, input_json):
        conn,cursor = self.connect_to_database()
        key_words = input_json.get('key_words')

        searched_melon = self.search_melon(conn,cursor,key_words)

        output_dict = dict()
        output_dict['event_list'] = searched_melon
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
    
    def search_melon(self,conn,cursor,key_words):
        searched_melon = list()
        event_list = self.get_event_list(conn,cursor)
        for event in event_list:
            flag = 1
            for key in key_words:
                if key not in event['event_top_word']:
                    flag = 0
                    break
            if flag == 1:
                del event['event_top_word']
                searched_melon.append(event)
        return searched_melon
    
    def get_event_list(self,conn,cursor):
        event_list_sql = '''
        SELECT 
            ie.event_id,
            ie.pic_url,
            ie.abstract_title,
            eli.event_top_word
        FROM
            melonfield.important_event ie
                JOIN
            melonfield.event_latest_info eli 
                ON 
            eli.event_id = ie.event_id
        ORDER BY eli.ts_in_min DESC;
        '''
        cursor.execute(event_list_sql)
        result = cursor.fetchall()
        event_list = [dict(zip(('event_id','pic_url','abstract_title','event_top_word'),x)) for x in result]
        return event_list