# -*- coding: utf-8 -*-
import pymysql
from datetime import datetime,timedelta
import requests
from collections import defaultdict

class load_model():
    def __init__(self):
        self.model_name = 'event_module_model'
        self.model_version = '1.0'
        self.type_module_rank = dict()
        self.type_module_rank[0] = [1,2,6,7,4,3,5]
        self.type_module_rank[1] = [3,2,1,7,6,4,5]
        self.type_module_rank[2] = [1,2,6,7,4,3,5]
        self.type_module_rank[3] = [6,1,2,4,7,3,5]
        self.type_module_rank[4] = [4,2,1,6,7,3,5]
        
        self.module_name = dict()
        self.module_name[0] = '时间线'
        self.module_name[1] = '观点'
        self.module_name[2] = '点评'
        self.module_name[3] = '白皮书'
        self.module_name[4] = '各地'
        self.module_name[5] = '相关推荐'
        self.module_name[6] = '小百科'
        self.module_name[7] = '小贴士'
        
    def process(self, input_json):
        event_id = input_json.get('event_id')        
        conn,cursor = self.connect_to_database()
        
        event_module = self.get_event_module(cursor, event_id)
        
        output_dict = dict()
        output_dict['event_module'] = event_module
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

    def get_event_module(self, cursor, event_id):
        event_module_info_sql = '''
            SELECT 
                nm.news_module,
                ern.content_id,
                DATE_FORMAT(nb.publish_time, '%%Y-%%m-%%d %%H:%%i:%%S'),
                na.title,
                na.abstract,
                ie.event_type
            FROM
                melonfield.event_related_news ern
                    JOIN
                melonfield.news_module nm ON ern.content_id = nm.content_id
                    JOIN
                melonfield.news_abstract na ON ern.content_id = na.content_id
                    JOIN
                streaming.news_basics nb on ern.content_id = nb.content_id
					JOIN
				melonfield.important_event ie on ern.event_id = ie.event_id
            where ern.event_id = %s order by news_module asc,publish_time desc;      
        '''
        cursor.execute(event_module_info_sql,(event_id,))
        result = cursor.fetchall()
        full_event_module_info = defaultdict(list)
        event_module_info = dict()
        if result:
            for row in result:
                module = row[0]
                event_type = row[5]
                full_event_module_info[module].append(dict(zip(('content_id','publish_time','title','abstract'),row[1:5])))
            
            for module_type in self.type_module_rank[event_type]:
                module_name = self.module_name[module_type]
                event_module_info[module_name] = full_event_module_info[module_type]
        return event_module_info