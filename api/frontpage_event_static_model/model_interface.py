# -*- coding: utf-8 -*-
import pymysql
from datetime import datetime,timedelta

class load_model():
    def __init__(self):
        self.model_name = 'frontpage_event_static_model'
        self.model_version = '1.0'
        
    def process(self, input_json):
        conn,cursor = self.connect_to_database()
        event_info = self.get_event_info(cursor)
        
        output_dict = dict()
        output_dict['event_info'] = event_info
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
    

    def get_event_info(self,cursor):
        # 328 375 337 29 346
        event_str = '721,745,360,615,749,732,572,738,407,649,395,635,724,715,713,711,694,684,304,70,309,115,400,141,261,145,393,369,229,335,311,355,167,391,397,374,330,322,600,395,723'
        special_basic_info_sql = '''
        select eli.event_id,ie.abstract_title,ie.pic_url,ie.event_type from melonfield.event_latest_info eli join melonfield.important_event ie on eli.event_id = ie.event_id where eli.event_id in ({}) order by eli.ts_in_min desc
        '''.format(event_str)
        cursor.execute(special_basic_info_sql)
        result = cursor.fetchall()
        event_info = [dict(zip(('event_id','abstract_title','pic_url','event_type'),x)) for x in result]

        return event_info       

        