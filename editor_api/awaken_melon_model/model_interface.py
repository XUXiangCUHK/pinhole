# -*- coding: utf-8 -*-
import pymysql
from datetime import datetime,timedelta
import requests
from melon_module.point_event import Point_event

class load_model():
    def __init__(self):
        self.model_name = 'awaken_melon_model'
        self.model_version = '1.0'
        self.P = Point_event()
        
    def process(self, input_json):
        conn,cursor = self.connect_to_database()
        event_id = input_json.get('event_id')
        start_time = input_json.get('start_time')

        status = self.awaken_melon(conn, cursor, event_id, start_time)

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
    
    def awaken_melon(self, conn, cursor, event_id, start_time):
        start_time = str(start_time)[0:4] + '-' + str(start_time)[4:6] + '-' + str(start_time)[6:8] + ' ' + str(start_time)[8:10] + ':' + str(start_time)[10:12] + ':00'
        try:
            self.P.mimic_tag_process(event_id, start_time)
            return 'success'
        except Exception as e:
            print(e)
            return 'fail'