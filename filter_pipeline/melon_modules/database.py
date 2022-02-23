# -*- coding: utf-8 -*-

# This file contain basic functions to interact with database
# +------------------------+
# | Tables_in_melonfield   |
# +------------------------+
# | entity_info            |
# | entity_related_event   |
# | event_info             |
# | event_latest_info      |
# | event_related_news     |
# | filtered_news_entity   |
# | filtered_news_info     |
# | important_event        |
# | important_news         |
# | news_abstract          |
# | news_preprocess_result |
# | recent_hot_news        |
# | special_info           |
# | special_related_event  |
# +------------------------+

import json
import pymysql
from contextlib import closing
from datetime import datetime, timedelta
from collections import Counter

class Database:
    def __init__(self):
        self.mysql_host = 'rm-wz9lh12zwnbo4b457.mysql.rds.aliyuncs.com'
        self.mysql_port = 3306
        self.mysql_user = 'melonfield'
        self.mysql_password = 'melonfield@DG_2020'

        self.time_now = datetime.now()
    
    def read_from_mysql(self, statement):
        mysql_connection = pymysql.connect(host=self.mysql_host, port=self.mysql_port, user=self.mysql_user, passwd=self.mysql_password)
        with closing(mysql_connection.cursor()) as cursor:
            cursor.execute(statement)
            select_query_result = cursor.fetchall()
        mysql_connection.close()
        return select_query_result
    
    def write_into_mysql(self, data, statement):
        if data:
            mysql_connection = pymysql.connect(host=self.mysql_host, port=self.mysql_port, user=self.mysql_user, passwd=self.mysql_password)
            with closing(mysql_connection.cursor()) as cursor:
                cursor.executemany(statement, data)
                mysql_connection.commit()
            mysql_connection.close()
        
    def execute_mysql(self, statement, data):
        mysql_connection = pymysql.connect(host=self.mysql_host, port=self.mysql_port, user=self.mysql_user, passwd=self.mysql_password)
        with closing(mysql_connection.cursor()) as cursor:
            cursor.execute(statement, data)
            mysql_connection.commit()
        mysql_connection.close()
    
    def get_time_interval(self, end_time, gap=24*60):
        start_time = end_time - timedelta(minutes=gap)
        start_time_str = start_time.strftime("%Y-%m-%d %H:%M")
        end_time_str = end_time.strftime("%Y-%m-%d %H:%M")
        return start_time_str, end_time_str
    
    def insert_entity_info_json(self, data):
        statement = '''
                    INSERT IGNORE INTO `melonfield`.`entity_info`
                    (`entity_id`, `entity_name`, `entity_type`)
                    VALUES
                    (%(entity_id)s, %(entity_name)s, %(entity_type)s);
                    '''
        self.write_into_mysql(data, statement)
    
    def insert_filtered_news_entity_json(self, data):
        statement = '''
                    INSERT IGNORE INTO `melonfield`.`filtered_news_entity`
                    (`content_id`, `entity_id`, `title_flag`, `count`)
                    VALUES
                    (%(content_id)s, %(entity_id)s, %(title_flag)s, %(count)s);
                    '''
        self.write_into_mysql(data, statement)
    
    def insert_filtered_news_info_json(self, data):
        statement = '''
                    INSERT IGNORE INTO `melonfield`.`filtered_news_info`
                    (`publish_time`, `content_id`, `tf_title`, `tf_content`, `top_word_per_news`, `repost_num`)
                    VALUES
                    (%(publish_time)s, %(content_id)s, %(tf_title)s, %(tf_content)s, %(top_word_per_news)s, %(repost_num)s);
                    '''
        self.write_into_mysql(data, statement)
    
    def insert_news_preprocess_result_json(self, data):
        statement = '''
                    INSERT IGNORE INTO `melonfield`.`news_preprocess_result`
                    (`content_id`, `segged_title`, `segged_content`)
                    VALUES
                    (%(content_id)s, %(segged_title)s, %(segged_content)s);
                    '''
        self.write_into_mysql(data, statement)
    
    # entity_info
    def get_initial_entity_id(self):
        statement = "SELECT max(entity_id) FROM melonfield.entity_info;"
        select_query_result = self.read_from_mysql(statement)
        if select_query_result:
            initial_entity_id = select_query_result[0][0]
            if initial_entity_id == None:
                return 0
            else:
                return initial_entity_id
        else:
            return 0

    def from_entity_id_to_name(self, entity_id):
        statement = "SELECT entity_name FROM melonfield.entity_info WHERE entity_id = '{}';".format(entity_id)
        select_query_result = self.read_from_mysql(statement)
        if select_query_result:
            return select_query_result[0][0]
        else:
            return str()
    
    def from_entity_name_to_id(self, entity_name):
        statement = '''SELECT entity_id FROM melonfield.entity_info WHERE entity_name="{}";'''.format(entity_name)
        try:
            select_query_result = self.read_from_mysql(statement)
            if select_query_result:
                return select_query_result[0][0]
            else:
                return -1
        except:
            return -1
    
    # news_publish_relation
    def get_repost_num(self, content_id):
        post_num = 0
        statement = "SELECT count(*) from streaming.news_publish_relation where content_id = '{}';".format(content_id)
        select_query_result = self.read_from_mysql(statement)
        for item in select_query_result:
            post_num = item[0]
        return post_num
