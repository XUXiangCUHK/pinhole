# -*- coding: utf-8 -*-
import pymysql
from datetime import datetime,timedelta
import requests
import re

class load_model():
    def __init__(self):
        self.model_name = 'cluster_abstract_model'
        self.model_version = '1.0'
        self.opinion_service_url = 'http://120.79.228.104:16006/get_news_opinions'

    def process(self, input_json):
        conn,cursor = self.connect_to_database()
        content_id_list = input_json.get('content_id_list')
        content_id_str = str([content_id_list])

        cluster_abstract = self.get_cluster_abstract(content_id_str)

        output_dict = dict()
        output_dict['cluster_abstract'] = cluster_abstract
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

    
    def get_cluster_abstract(self,news_id_str):
        input_dict = {
                'news_ids': news_id_str
                }
        r = self.svc(self.opinion_service_url, input_dict)
        if r:
            return r['result'][0]
        else:
            return ''