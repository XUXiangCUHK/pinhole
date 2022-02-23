# -*- coding: utf-8 -*-
import pymysql
from datetime import datetime,timedelta
import requests
import re
from gensim import similarities
from baidu_summary import Baidu_NLP

class load_model():
    def __init__(self):
        self.model_name = 'baidu_abstract_model'
        self.model_version = '1.0'

    def process(self, input_json):
        conn,cursor = self.connect_to_database()
        content_id = input_json.get('content_id')
        abstract_len = input_json.get('abstract_len')
        title, content = self.get_data(cursor,content_id)
        if content:
            baidu_abstract = self.get_baidu_abstract(title, content[:2900], abstract_len)
        else:
            baidu_abstract = ''
        output_dict = dict()
        output_dict['baidu_abstract'] = baidu_abstract
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
    
    def get_data(self, cursor, content_id):
        select_sql = '''
            SELECT 
                nbr.publish_id,nbr.title,nbr.content
            FROM
            	streaming.news_basics_raw nbr
            WHERE
                nbr.publish_id = %s;
        '''
        cursor.execute(select_sql,(content_id,))
        result = cursor.fetchall()
        title = result[0][1]
        content = result[0][2]
        return title, content

    def get_baidu_abstract(self, title, content, abstract_lens):
        API_KEY = 'Po5IwLHcVdvalD3mtb0F8cKG'
        SECRET_KEY = 'zkN29vY0E4vW6ZIXEiNxYDcdD6z8Gslh'
        baidu_nlp_cls = Baidu_NLP(API_KEY, SECRET_KEY)
        # publish_id = 'ec51c000a1c7151a'
        baidu_abstract = baidu_nlp_cls.make_summary_request(title, content, abstract_lens)
        return baidu_abstract
        

