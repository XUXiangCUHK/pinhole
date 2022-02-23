# -*- coding: utf-8 -*-
import pymysql
from datetime import datetime,timedelta
import requests

class load_model():
    def __init__(self):
        self.model_name = 'edit_news_model'
        self.model_version = '1.0'
        
    def process(self, input_json):
        conn,cursor = self.connect_to_database()
        content_id = input_json.get('content_id')
        title = input_json.get('title')
        short_abstract = input_json.get('short_abstract')
        abstract = input_json.get('abstract')
        highlight = input_json.get('highlight')

        status = self.update_news(conn,cursor,content_id,title,short_abstract,abstract,highlight)

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
    
    def update_news(self,conn,cursor,content_id,title,short_abstract,abstract,highlight):
        update_news_sql = '''
        REPLACE INTO melonfield.news_abstract (content_id,title,short_abstract,abstract,highlight) VALUES (%s,%s,%s,%s,%s);
        '''
        try:
            cursor.execute(update_news_sql,(content_id,title,short_abstract,abstract,highlight,))
            conn.commit()
            return 'success'    
        except:
            return 'fail'
        
    

