# -*- coding: utf-8 -*-
import pymysql
from datetime import datetime,timedelta
import requests
from event_generate.story_timeline_manipulator import story_timeline_manipulator
from collections import Counter

class load_model():
    def __init__(self):
        self.model_name = 'event_type_model'
        self.model_version = '1.0'
        self.stm = story_timeline_manipulator()

    def process(self, input_json):
        conn,cursor = self.connect_to_database()
        event_id = input_json.get('event_id')
        abstract_title = input_json.get('abstract_title')
        pic_url = input_json.get('pic_url')
        event_type = input_json.get('event_type')
        direction_guide = input_json.get('direction_guide')
        
        result = self.check_is_important(cursor, event_id)
        if not result:
            self.stm.regenerate_story_timeline(event_id, event_type)
        elif event_type != result[2] and event_type >= 0:
            self.stm.regenerate_story_timeline(event_id, event_type)
        
        if result:
            if not abstract_title:
                abstract_title = result[0]
            if not pic_url:
                pic_url = result[1]
            if event_type < 0:
                event_type = result[2]
            if not direction_guide:
                if result[3]:
                    direction_guide = result[3]

        status = self.update_event(conn,cursor,event_id, abstract_title, pic_url, event_type, direction_guide)

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
    
    def check_is_important(self, cursor, event_id):
        check_sql = '''
            SELECT
                abstract_title, pic_url, event_type, direction_guide
            FROM
                melonfield.important_event
            WHERE
                event_id = %s
        '''
        cursor.execute(check_sql, (event_id, ))
        result = cursor.fetchall()
        if len(result)==1:
            return result[0]
        else:
            return None

    def update_event(self,conn,cursor,event_id, abstract_title, pic_url, event_type, direction_guide):
        mark_time = datetime.now()
        if not pic_url:
            pic_url = 'https://st2.depositphotos.com/6935564/11527/i/950/depositphotos_115274062-stock-photo-seamless-fresh-juicy-ripe-watermelon.jpg'
        
        update_event_sql = '''REPLACE INTO melonfield.important_event (event_id, mark_time, abstract_title, pic_url, event_type, direction_guide) VALUES (%s, %s, %s, %s, %s, %s);'''

        try:
            cursor.execute(update_event_sql,(event_id, mark_time, abstract_title, pic_url, event_type, direction_guide,))
            conn.commit()
            return 'success'    
        except:
            return 'fail'

