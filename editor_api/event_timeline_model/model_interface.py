# -*- coding: utf-8 -*-
import pymysql
from datetime import datetime,timedelta
import requests

class load_model():
    def __init__(self):
        self.model_name = 'event_timeline_model'
        self.model_version = '1.0'
        
    def process(self, input_json):
        conn,cursor = self.connect_to_database()
        event_id = input_json.get('event_id')        
        event_timeline = self.get_event_timeline(conn,cursor,event_id)

        output_dict = dict()
        output_dict['event_timeline'] = event_timeline
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
    
    def get_event_timeline(self,conn,cursor,event_id):
        event_timeline_sql = '''
            SELECT 
                etn1.content_id, DATE_FORMAT(publish_time,"%%Y-%%m-%%d %%H:%%i:%%S"), title
            FROM
                melonfield.event_timeline_news etn1
                    LEFT JOIN
                melonfield.event_timeline_news etn2 
                ON etn1.event_id = etn2.event_id AND etn1.ts_in_min < etn2.ts_in_min
            		JOIN
            	streaming.news_basics nb
                ON etn1.content_id = nb.content_id
            WHERE
                etn1.event_id = %s
                    AND ISNULL(etn2.ts_in_min) order by publish_time desc;
        '''
        cursor.execute(event_timeline_sql,(event_id,))
        result = cursor.fetchall()
        event_timeline = [dict(zip(('content_id','publish_time','title'),x)) for x in result]
        return event_timeline
    
