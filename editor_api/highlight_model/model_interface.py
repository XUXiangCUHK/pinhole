# -*- coding: utf-8 -*-
import pymysql
from datetime import datetime,timedelta
from textrank_highlight import Highlight
import requests

class load_model():
    def __init__(self):
        self.model_name = 'highlight_model'
        self.model_version = '1.0'
        
    def process(self, input_json):
        conn,cursor = self.connect_to_database()
        text = input_json.get('text')

        highlight = self.get_highlight(text)

        output_dict = dict()
        output_dict['highlight'] = highlight
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
    
    def get_highlight(self,text):
        H = Highlight()
        return H.get_highlight_sent(text, sen_num=2) # default as 2