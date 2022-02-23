# -*- coding: utf-8 -*-
import pymysql
from datetime import datetime,timedelta
import requests

class load_model():
    def __init__(self):
        self.model_name = 'edit_recommendation_model'
        self.model_version = '1.0'
        
    def process(self, input_json):
        conn,cursor = self.connect_to_database()
        event_id = input_json.get('event_id')
        recommend_event_id_list = input_json.get('recommend_event_id_list')

        status = self.update_table(conn, cursor, event_id, recommend_event_id_list)

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
    
    def update_table(self, conn, cursor, event_id, recommend_event_id_list):
        update_news_sql = '''
        INSERT IGNORE INTO melonfield.event_recommendation (event_id, recommend_event_id) VALUES (%s, %s);
        '''
        try:
            for item in recommend_event_id_list:
                cursor.execute(update_news_sql, (event_id, item, ))
            conn.commit()
            return 'success'    
        except:
            return 'fail'