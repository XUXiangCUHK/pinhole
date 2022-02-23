# -*- coding: utf-8 -*-
import pymysql
from datetime import datetime,timedelta
import requests
import re
from gensim import similarities
from baidu_summary import Baidu_NLP

class load_model():
    def __init__(self):
        self.model_name = 'news_abstract_model'
        self.model_version = '1.0'
        self.sentSplitRe = re.compile(r'[^?!。？！]+(?:[?!。？！]+[”]?|$)')
        self.news_summary_url = 'http://47.106.46.242:30009/calculate'
        self.opinion_service_url = 'http://120.79.228.104:16006/get_news_opinions'
        
    def process(self, input_json):
        conn,cursor = self.connect_to_database()
        content_id = input_json.get('content_id')
        title, content = self.get_data(cursor,content_id)
        if content:
            first_three_sent = self.get_first_three_sent(content)
            abstract = self.get_abstract(content)
            short_baidu_abstract, long_baidu_abstract = self.get_baidu_abstract(title, content)
        else:
            short_baidu_abstract = ''
            long_baidu_abstract = ''
            first_three_sent = ''
            abstract = ''
        output_dict = dict()
        output_dict['first_three_sent'] = first_three_sent
        output_dict['abstract'] = abstract
        output_dict['short_baidu_abstract'] = short_baidu_abstract
        output_dict['long_baidu_abstract'] = long_baidu_abstract
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
        
    def get_first_three_sent(self,content):
        first_three_sent = ''
        sents = self.sentSplitRe.findall(content)
        for idx, sent in enumerate(sents):
            sent = sent.strip()
            sentLen = len(sent)
            if (idx < 3 or idx > len(sents) - 3) and sentLen < 5:
                continue
            if idx >= 4:
                break
            first_three_sent += sent
        return first_three_sent
    
    def get_abstract(self, content):
        news_summary_svc_input = {
                'model_name': 'news_summary',
                'model_version': '1.0',
                'raw_text': content,
                "limit":4,
                "process_raw":True
        }
        r = self.svc(self.news_summary_url,news_summary_svc_input)
        return r['summary']

    def get_baidu_abstract(self, title, content):
        API_KEY = 'Po5IwLHcVdvalD3mtb0F8cKG'
        SECRET_KEY = 'zkN29vY0E4vW6ZIXEiNxYDcdD6z8Gslh'
        baidu_nlp_cls = Baidu_NLP(API_KEY, SECRET_KEY)
        # publish_id = 'ec51c000a1c7151a'
        max_summary_len = 300 # 200 if absent
        long_baidu_abstract = baidu_nlp_cls.make_summary_request(title, content, max_summary_len)
        max_summary_len = 150 # 200 if absent
        short_baidu_abstract = baidu_nlp_cls.make_summary_request(title, content, max_summary_len)
        return short_baidu_abstract, long_baidu_abstract
        

