# -*- coding: utf-8 -*-
import pymysql
from datetime import datetime,timedelta

class load_model():
    def __init__(self):
        self.model_name = 'event_timeline_model'
        self.model_version = '1.0'
        
    def process(self, input_json):
        event_id = input_json.get('event_id', None)
        conn,cursor = self.connect_to_database()
        
        event_info = self.get_event_info(cursor,event_id)
        
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

    def get_event_info(self,cursor,event_id):
        ts_in_min_sql = '''
        SELECT ts_in_min FROM melonfield.event_timeline_news WHERE event_id = %s AND is_edited = 0 ORDER BY ts_in_min DESC LIMIT 1;
        '''

        news_abstract_sql = '''
            SELECT 
                t.content_id,
                b.title,
                DATE_FORMAT(b.publish_time, '%%Y-%%m-%%d %%H:%%i:%%S'),
                a.abstract,
                a.short_abstract,
                a.highlight,
                b.url,
                b.source_name
            FROM
                melonfield.event_timeline_news t
            JOIN
                streaming.news_basics b ON b.content_id = t.content_id
            LEFT JOIN
                melonfield.news_abstract a ON t.content_id = a.content_id
            WHERE t.event_id = %s AND t.ts_in_min = %s
            ORDER BY b.publish_time DESC;      
        '''
        cursor.execute(ts_in_min_sql,(event_id,))
        result = cursor.fetchall()
        ts_in_min = result[0][0]

        cursor.execute(news_abstract_sql,(event_id,ts_in_min,))
        result = cursor.fetchall()
        # event_info = [dict(zip(('content_id','title','publish_time','abstract','short_abstract','highlight'),x)) for x in result]
        event_info = list()
        for row in result:
            if row[5]:
                highlight = row[5].split('|')
            else:
                highlight = str()
            url = row[6]
            if url.startswith('20') or url == 'UNKNOWN':
                url = str()
            event_info.append({
                'content_id': row[0],
                'title': row[1],
                'publish_time': row[2],
                'abstract': row[3],
                'short_abstract': row[4],
                'highlight': highlight,
                'url': url,
                'source_name': row[7]
            })
        
        for i in range(0, len(event_info)):
            if i == 0:
                event_info[i]['logo'] = 1
            elif i < len(event_info)/2:
                event_info[i]['logo'] = 2
            else:
                event_info[i]['logo'] = 3
        return event_info       




        
        
        
        
        