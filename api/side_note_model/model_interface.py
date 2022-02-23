# -*- coding: utf-8 -*-
import pymysql
from datetime import datetime,timedelta

class load_model():
    def __init__(self):
        self.model_name = 'side_note_model'
        self.model_version = '1.0'
        
    def process(self, input_json):
        event_id = input_json.get('event_id', None)
        content_id = input_json.get('content_id', None)
        conn,cursor = self.connect_to_database()
        
        note_info = self.get_note_info(cursor,event_id,content_id)
        
        output_dict = dict()
        output_dict['note_info'] = note_info
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

    def get_note_info(self,cursor,event_id,content_id):
        note_info_event_id_sql = '''
        SELECT
            note_id, note_title, note_url, note_text, text_type
        FROM
            melonfield.side_note
        WHERE
            event_id = %s AND content_id = 'EVENT';
        '''
        note_info_content_id_sql = '''
        SELECT
            note_id, note_title, note_url, note_text, text_type
        FROM
            melonfield.side_note
        WHERE
            event_id = %s AND content_id = %s;       
        '''
        if content_id:
            cursor.execute(note_info_content_id_sql,(event_id,content_id,))
            result = cursor.fetchall()
        else:
            cursor.execute(note_info_event_id_sql,(event_id,))
            result = cursor.fetchall()

        note_info = list()
        for row in result:
            note_id = row[0]
            note_title = row[1]
            note_url = row[2]
            note_text = row[3]
            text_type = row[4]
            if text_type == 4:
                note_title = list(row[1])
            note_info.append({
                'note_id': note_id,
                'note_title': note_title,
                'note_url': note_url,
                'note_text': note_text,
                'text_type': text_type,
            })
        # note_info = [dict(zip(('note_id','note_title', 'note_url', 'note_text','text_type'),x)) for x in result]
        return note_info       




        
        
        
        
        