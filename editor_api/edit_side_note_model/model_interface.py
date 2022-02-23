# -*- coding: utf-8 -*-
import pymysql
from datetime import datetime,timedelta
import requests
from hashlib import md5

class load_model():
    def __init__(self):
        self.model_name = 'edit_side_note_model'
        self.model_version = '1.0'
        
    def process(self, input_json):
        conn,cursor = self.connect_to_database()
        event_id = input_json.get('event_id')
        content_id = input_json.get('content_id')
        note_title = input_json.get('note_title')
        note_url = input_json.get('note_url')
        note_text = input_json.get('note_text')
        text_type = input_json.get('text_type')

        status = self.update_news(conn,cursor,event_id,content_id,note_title, note_url, note_text,text_type)

        output_dict = dict()
        output_dict['status'] = status
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

    def get_note_internal_id(self, content):
        return md5(content.encode('utf-8')).hexdigest()
    
    def update_news(self,conn,cursor,event_id,content_id,note_title, note_url, note_text,text_type):
        if not content_id:
            content_id = 'EVENT'
        note_id = self.get_note_internal_id(str(event_id)+content_id+note_title+note_text)
        update_news_sql = '''
        REPLACE INTO melonfield.side_note (note_id,event_id,content_id,note_title, note_url, note_text,text_type) VALUES (%s,%s,%s,%s,%s,%s,%s);
        '''
        try:
            cursor.execute(update_news_sql,(note_id,event_id,content_id,note_title, note_url, note_text,text_type,))
            conn.commit()
            return 'success'    
        except:
            return 'fail'