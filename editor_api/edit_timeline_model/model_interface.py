# -*- coding: utf-8 -*-
import pymysql
from datetime import datetime,timedelta
import requests
from melon_module.abstract import abstract

class load_model():
    def __init__(self):
        self.model_name = 'edit_timeline_model'
        self.model_version = '1.0'
        self.abt = abstract()
        
    def process(self, input_json):
        conn,cursor = self.connect_to_database()
        event_id = input_json.get('event_id')
        content_id = input_json.get('content_id')
        mode = input_json.get('mode')

        status = self.edit_timeline(conn, cursor, event_id, content_id, mode)

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
    
    def edit_timeline(self, conn, cursor, event_id, content_id, mode):
        add_news_into_timeline_sql = '''
        INSERT IGNORE INTO melonfield.event_timeline_news (event_id, content_id, is_edited, ts_in_min) VALUES (%s,%s,%s,%s);
        '''
        add_news_abstract_sql = '''
        INSERT IGNORE INTO melonfield.news_abstract (content_id, title, short_abstract, abstract, highlight) VALUES (%s, %s, %s, %s, %s);
        '''
        remove_news_from_timeline_sql = '''
        DELETE FROM melonfield.event_timeline_news
        WHERE event_id = %s and content_id = %s and ts_in_min = %s;
        '''
        try:
            is_edited = 1
            ts_in_min = self.get_ts_in_min(cursor, event_id)
            if mode == 'add':
                news_info = self.get_news_info(cursor, content_id)
                if news_info:
                    title = news_info[0]
                    content = news_info[1]
                short_abstract = self.abt.get_news_abstract(title, content)
                abstract = self.abt.get_news_abstract(title, content, 300)
                highlight = self.abt.get_highlight_sent(abstract)
                cursor.execute(add_news_abstract_sql, (content_id, title, short_abstract, abstract, highlight, ))
                cursor.execute(add_news_into_timeline_sql,(event_id, content_id, is_edited, ts_in_min,))
            if mode == 'remove':
                cursor.execute(remove_news_from_timeline_sql,(event_id, content_id, ts_in_min,))
            conn.commit()
            return 'success'    
        except Exception as e:
            print(e)
            return 'fail'
    
    def get_news_info(self, cursor, content_id):
        get_news_info_sql = '''
        SELECT title, content FROM streaming.news_basics_raw WHERE publish_id = %s
        '''
        cursor.execute(get_news_info_sql,(content_id,))
        result = cursor.fetchall()
        if len(result)==1:
            return result[0]
        else:
            return None

    def get_ts_in_min(self, cursor, event_id):
        get_ts_in_min_sql = '''
        SELECT ts_in_min
        FROM melonfield.event_timeline_news
        WHERE event_id = %s
        ORDER BY id DESC
        LIMIT 1;
        '''
        cursor.execute(get_ts_in_min_sql,(event_id,))
        result = cursor.fetchall()
        for row in result:
            ts_in_min = row[0]
        return ts_in_min
