# -*- coding: utf-8 -*-
import pymysql
from datetime import datetime,timedelta

class load_model():
    def __init__(self):
        self.model_name = 'express_model'
        self.model_version = '1.0'
        
    def process(self, input_json):
        conn,cursor = self.connect_to_database()
        express = self.get_express(cursor)
        
        output_dict = dict()
        output_dict['express'] = express
        cursor.close()
        conn.close()
        return output_dict

    def get_version(self):
        return self.model_version

    def get_name(self):
        return self.model_name

    def connect_to_database(self):
        conn = pymysql.connect(
        host='rm-wz9lh12zwnbo4b457.mysql.rds.aliyuncs.com',port=3306,
        user='melonfield',password='melonfield@DG_2020',
        charset='utf8mb4')
        cursor = conn.cursor()
        return conn,cursor
    
    def get_express(self,cursor):
        express_sql = '''
        select content_id,DATE_FORMAT(publish_time,"%Y-%m-%d %H:%i:%S"),title,content from melonfield.important_news order by publish_time desc limit 50
        '''
        cursor.execute(express_sql)
        result = cursor.fetchall()
        express = [dict(zip(('content_id','publish_time','title','content'),x)) for x in result]
        return express       
        
        
        
        
        