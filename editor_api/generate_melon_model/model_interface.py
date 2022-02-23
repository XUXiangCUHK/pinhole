# -*- coding: utf-8 -*-
import pymysql
from datetime import datetime,timedelta
import requests
from melon_module.point_event import Point_event

class load_model():
    def __init__(self):
        self.model_name = 'generate_melon_model'
        self.model_version = '1.0'
        self.P = Point_event()
        
    def process(self, input_json):
        conn,cursor = self.connect_to_database()
        key_words = input_json.get('key_words')
        operator = input_json.get('operator')
        minimum_should_match = input_json.get('minimum_should_match')
        only_title = input_json.get('only_title')
        start_time = input_json.get('start_time')
        end_time = input_json.get('end_time')
        mode = input_json.get('mode')

        event_id = self.generate_melon(conn, cursor, key_words, operator, minimum_should_match, only_title, start_time, end_time, mode)

        output_dict = dict()
        output_dict['event_id'] = event_id
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
    
    def generate_melon(self, conn, cursor, key_words, operator, minimum_should_match, only_title, begin, end, mode):
        begin = str(begin)[0:4] + '-' + str(begin)[4:6] + '-' + str(begin)[6:8] + ' ' + str(begin)[8:10] + ':' + str(begin)[10:12] + ':00'
        end = str(end)[0:4] + '-' + str(end)[4:6] + '-' + str(end)[6:8] + ' ' + str(end)[8:10] + ':' + str(end)[10:12] + ':00'
        # period = begin + ' ' + end
        # event_id = self.P.find_all_related_news(key_words, period)
        event_id = self.P.ES_search(key_words, operator, minimum_should_match, only_title, begin, end)
        if event_id > 0 and mode == 'evolve':
            self.P.mimic_tag_process(event_id, end)
        return event_id