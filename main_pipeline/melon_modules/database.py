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
    
    def insert_entity_info(self, data):
        statement = '''
                    INSERT IGNORE INTO `melonfield`.`entity_info`
                    (`entity_id`, `entity_name`)
                    VALUES
                    (%(entity_id)s, %(entity_name)s);
                    '''
        self.write_into_mysql(data, statement)

    def insert_entity_related_event(self, data):
        statement = '''
                    INSERT IGNORE INTO `melonfield`.`entity_related_event`
                    (`entity_id`, `event_id`, `ts_in_min`)
                    VALUES
                    (%(entity_id)s, %(event_id)s, %(ts_in_min)s);
                    '''
        self.write_into_mysql(data, statement)
    
    def insert_event_info(self, data):
        statement = '''
                    INSERT IGNORE INTO `melonfield`.`event_info`
                    (`event_id`, `ts_in_min`, `news_num`, `event_score`, `event_top_word`, `abstract_title`, `abstract_content`, `leaf_content_id_list`)
                    VALUES
                    (%(event_id)s, %(ts_in_min)s, %(news_num)s, %(event_score)s, %(event_top_word)s, %(abstract_title)s, %(abstract_content)s, %(leaf_content_id_list)s);
                    '''
        self.write_into_mysql(data, statement)
    
    def insert_event_info_flag(self, data):
        statement = '''
                    INSERT IGNORE INTO `melonfield`.`event_info`
                    (`event_id`, `ts_in_min`, `news_num`, `event_score`, `event_top_word`, `abstract_title`, `abstract_content`, `leaf_content_id_list`, `is_manual`)
                    VALUES
                    (%(event_id)s, %(ts_in_min)s, %(news_num)s, %(event_score)s, %(event_top_word)s, %(abstract_title)s, %(abstract_content)s, %(leaf_content_id_list)s, %(is_manual)s);
                    '''
        self.write_into_mysql(data, statement)
    
    # Replace into
    def insert_event_latest_info(self, data):
        statement = '''
                    REPLACE INTO `melonfield`.`event_latest_info`
                    (`event_id`, `ts_in_min`, `news_num`, `event_score`, `event_top_word`, `abstract_title`, `abstract_content`, `leaf_content_id_list`)
                    VALUES
                    (%(event_id)s, %(ts_in_min)s, %(news_num)s, %(event_score)s, %(event_top_word)s, %(abstract_title)s, %(abstract_content)s, %(leaf_content_id_list)s);
                    '''
        self.write_into_mysql(data, statement)
    
    def update_event_latest_info(self, event_id, update_data, column='leaf_content_id_list'):
        statement = '''UPDATE `melonfield`.`event_latest_info` SET {} = %s WHERE event_id = %s;'''.format(column)
        self.execute_mysql(statement, (update_data, event_id))
    
    def delete_event_latest_info(self, event_id):
        statement = "DELETE FROM melonfield.event_latest_info where event_id = %s;"
        self.execute_mysql(statement, (event_id))
    
    def insert_event_related_news(self, data):
        statement = '''
                    INSERT IGNORE INTO `melonfield`.`event_related_news`
                    (`event_id`, `content_id`, `ts_in_min`)
                    VALUES
                    (%(event_id)s, %(content_id)s, %(ts_in_min)s);
                    '''
        self.write_into_mysql(data, statement)
    
    def insert_filtered_news_entity(self, data):
        statement = '''
                    INSERT IGNORE INTO `melonfield`.`filtered_news_entity`
                    (`content_id`, `entity_id`, `title_flag`, `count`)
                    VALUES
                    (%(content_id)s, %(entity_id)s, %(title_flag)s, %(count)s);
                    '''
        self.write_into_mysql(data, statement)
    
    def insert_filtered_news_info(self, data):
        statement = '''
                    INSERT IGNORE INTO `melonfield`.`filtered_news_info`
                    (`publish_time`, `content_id`, `tf_per_news`, `top_word_per_news`, `repost_num`)
                    VALUES
                    (%(publish_time)s, %(content_id)s, %(tf_per_news)s, %(top_word_per_news)s, %(repost_num)s);
                    '''
        self.write_into_mysql(data, statement)
    
    def insert_filtered_news_info_json(self, data):
        statement = '''
                    INSERT IGNORE INTO `melonfield`.`filtered_news_info_json`
                    (`publish_time`, `content_id`, `tf_per_news`, `top_word_per_news`, `repost_num`)
                    VALUES
                    (%(publish_time)s, %(content_id)s, %(tf_per_news)s, %(top_word_per_news)s, %(repost_num)s);
                    '''
        self.write_into_mysql(data, statement)
    
    def insert_important_event(self, data):
        statement = '''
                    INSERT IGNORE INTO `melonfield`.`important_event`
                    (`event_id`, `mark_time`, `pic_url`, `abstract_title`)
                    VALUES
                    (%(event_id)s, %(mark_time)s, %(pic_url)s, %(abstract_title)s);
                    '''
        self.write_into_mysql(data, statement)
    
    def update_important_event(self, event_id, abstract_title=None, pic_url=None):
        if abstract_title:
            statement = '''UPDATE `melonfield`.`important_event` SET abstract_title = %s WHERE event_id = %s;'''
            self.execute_mysql(statement, (abstract_title, event_id))
        if pic_url:
            statement = '''UPDATE `melonfield`.`important_event` SET pic_url = %s WHERE event_id = %s;'''
            self.execute_mysql(statement, (pic_url, event_id))

    def insert_important_news(self, data):
        statement = '''
                    INSERT IGNORE INTO `melonfield`.`important_news`
                    (`content_id`, `publish_time`, `title`, `content`)
                    VALUES
                    (%(content_id)s, %(publish_time)s, %(title)s, %(content)s);
                    '''
        self.write_into_mysql(data, statement)
    
    def insert_news_abstract(self, data):
        statement = '''
                    INSERT IGNORE INTO `melonfield`.`news_abstract`
                    (`content_id`, `title`, `abstract`)
                    VALUES
                    (%(content_id)s, %(title)s, %(abstract)s);
                    '''
        self.write_into_mysql(data, statement)
    
    def update_news_abstract(self, data):
        statement = '''
                    REPLACE INTO `melonfield`.`news_abstract`
                    (`content_id`, `title`, `abstract`)
                    VALUES
                    (%(content_id)s, %(title)s, %(abstract)s);
                    '''
        self.write_into_mysql(data, statement)
    
    def insert_news_preprocess_result(self, data):
        statement = '''
                    INSERT IGNORE INTO `melonfield`.`news_preprocess_result`
                    (`content_id`, `segged_title`, `segged_content`)
                    VALUES
                    (%(content_id)s, %(segged_title)s, %(segged_content)s);
                    '''
        self.write_into_mysql(data, statement)

    def insert_recent_hot_news(self, data):
        statement = '''
                    INSERT IGNORE INTO `melonfield`.`recent_hot_news`
                    (`content_id`, `hotness`, `abstract_title`, `ts_in_min`)
                    VALUES
                    (%(content_id)s, %(hotness)s, %(abstract_title)s, %(ts_in_min)s);
                    '''
        self.write_into_mysql(data, statement)
    
    def insert_special_info(self, data):
        statement = '''
                    INSERT IGNORE INTO `melonfield`.`special_info`
                    (`special_id`, `mark_time`, `pic_url`, `abstract_title`)
                    VALUES
                    (%(special_id)s, %(mark_time)s, %(pic_url)s, %(abstract_title)s);
                    '''
        self.write_into_mysql(data, statement)
    
    def update_special_info(self, special_id, abstract_title=None, pic_url=None):
        mark_time = datetime.now()
        if abstract_title:
            statement = '''UPDATE `melonfield`.`special_info` SET abstract_title = %s, mark_time = %s WHERE special_id = %s;'''
            self.execute_mysql(statement, (abstract_title, mark_time, special_id))
        if pic_url:
            statement = '''UPDATE `melonfield`.`special_info` SET pic_url = %s, mark_time = %s WHERE special_id = %s;'''
            self.execute_mysql(statement, (pic_url, mark_time, special_id))
    
    def delete_special_info(self, special_id):
        statement = "DELETE FROM melonfield.special_info where special_id = %s;"
        self.execute_mysql(statement, (special_id))
    
    def insert_special_related_event(self, data):
        statement = '''
                    INSERT IGNORE INTO `melonfield`.`special_related_event`
                    (`special_id`, `event_id`, `ts_in_min`)
                    VALUES
                    (%(special_id)s, %(event_id)s, %(ts_in_min)s);
                    '''
        self.write_into_mysql(data, statement)
    
    def delete_special_related_event(self, special_id, event_id=None):
        if event_id:
            statement = "DELETE FROM melonfield.special_related_event where special_id = %s and event_id = %s;"
            self.execute_mysql(statement, (special_id, event_id))
        else:
            statement = "DELETE FROM melonfield.special_related_event where special_id = %s;"
            self.execute_mysql(statement, (special_id))
    
    def insert_event_timeline_news(self, data):
        statement = '''
                    INSERT IGNORE INTO `melonfield`.`event_timeline_news`
                    (`event_id`, `content_id`, `is_edited`, `ts_in_min`)
                    VALUES
                    (%(event_id)s, %(content_id)s, %(is_edited)s, %(ts_in_min)s);
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
        select_query_result = self.read_from_mysql(statement)
        if select_query_result:
            return select_query_result[0][0]
        else:
            return -1

    # event_related_news
    def from_news_to_event_list(self, content_id):
        statement = "SELECT event_id FROM melonfield.event_related_news WHERE content_id = '{}';".format(content_id)
        select_query_result = self.read_from_mysql(statement)
        event_list = list()
        for item in select_query_result:
            event_list.append(item[0])
        return event_list
    
    def from_event_to_news_collection(self, event_id):
        statement = "SELECT content_id FROM melonfield.event_related_news WHERE event_id = '{}';".format(event_id)
        select_query_result = self.read_from_mysql(statement)
        news_collection = list()
        for item in select_query_result:
            news_collection.append(item[0])
        return news_collection
    
    def get_recent_event_id(self, ts_before):
        statement = "SELECT distinct event_id FROM melonfield.event_related_news WHERE ts_in_min > '{}';".format(ts_before)
        select_query_result = self.read_from_mysql(statement)
        event_id_list = list()
        for item in select_query_result:
            event_id_list.append(item[0])
        return event_id_list
    
    # entity_related_event
    def from_event_to_entity_id_list(self, event_id):
        statement = "SELECT entity_id FROM melonfield.entity_related_event WHERE event_id = '{}';".format(event_id)
        select_query_result = self.read_from_mysql(statement)
        entity_id_list = list()
        for item in select_query_result:
            entity_id_list.append(item[0])
        return entity_id_list
    
    # filtered_news_entity
    def get_news_entity_id_list(self, content_id):
        statement = "SELECT entity_id FROM melonfield.filtered_news_entity WHERE content_id = '{}';".format(content_id)
        select_query_result = self.read_from_mysql(statement)
        news_entity_id_list = list()
        for item in select_query_result:
            news_entity_id_list.append(item[0])
        return news_entity_id_list
    
    def get_news_entity_list(self, content_id):
        statement = "SELECT entity_id FROM melonfield.filtered_news_entity WHERE content_id = '{}';".format(content_id)
        select_query_result = self.read_from_mysql(statement)
        news_entity_list = list()
        for item in select_query_result:
            entity_id = item[0]
            entity_name = self.from_entity_id_to_name(entity_id)
            news_entity_list.append(entity_name)
        return news_entity_list
    
    # filtered_news_info
    def get_news_attribution(self, content_id):
        statement = "SELECT tf_per_news, top_word_per_news FROM melonfield.filtered_news_info WHERE content_id = '{}';".format(content_id)
        select_query_result = self.read_from_mysql(statement)
        if select_query_result:
            tf_per_news = eval(select_query_result[0][0])
            top_word_per_news = eval(select_query_result[0][1])
            return tf_per_news, top_word_per_news
        else:
            return Counter(), list()
    
    def get_news_attribution_json(self, content_id):
        statement = "SELECT tf_per_news, top_word_per_news FROM melonfield.filtered_news_info_json WHERE content_id = '{}';".format(content_id)
        select_query_result = self.read_from_mysql(statement)
        if select_query_result:
            tf_per_news = json.loads(select_query_result[0][0])
            top_word_per_news = json.loads(select_query_result[0][1])
            return tf_per_news, top_word_per_news
        else:
            return Counter(), list()
    
    # event_info
    def get_initial_event_id(self):
        statement = "SELECT max(event_id) FROM melonfield.event_info;"
        select_query_result = self.read_from_mysql(statement)
        if select_query_result:
            initial_event_id = select_query_result[0][0]
            if initial_event_id == None:
                return 0
            else:
                return initial_event_id
        else:
            return 0

    # event_latest_info
    def get_event_pool(self, ts_before):
        statement = "SELECT event_id, event_top_word, event_score FROM melonfield.event_latest_info where ts_in_min > '{}';".format(ts_before)
        select_query_result = self.read_from_mysql(statement)
        event_pool = dict()
        for item in select_query_result:
            event_id = item[0]
            entity_id_list = self.from_event_to_entity_id_list(event_id)
            event_top_word = eval(item[1])
            event_score = item[2]
            event_pool[event_id] = {
                'entity_id_list': entity_id_list,
                'event_top_word': event_top_word,
                'event_score': event_score
            }
        return event_pool
    
    def get_leaf_content_id_list(self, event_id):
        statement = "SELECT leaf_content_id_list FROM melonfield.event_latest_info where event_id = '{}';".format(event_id)
        select_query_result = self.read_from_mysql(statement)
        leaf_content_id_list = eval(select_query_result[0][0])
        return leaf_content_id_list
    
    def get_event_info(self, event_id):
        statement = "SELECT news_num, event_score, event_top_word, abstract_title, abstract_content, leaf_content_id_list FROM melonfield.event_latest_info where event_id = '{}';".format(event_id)
        select_query_result = self.read_from_mysql(statement)
        news_num = select_query_result[0][0]
        event_score = select_query_result[0][1]
        event_top_word = eval(select_query_result[0][2])
        abstract_title = select_query_result[0][3]
        abstract_content = select_query_result[0][4]
        leaf_content_id_list = eval(select_query_result[0][5])
        return news_num, event_score, event_top_word, abstract_title, abstract_content, leaf_content_id_list
    
    # news_basics
    def get_news_info(self, content_id):
        statement = "SELECT title, source_name, publish_time FROM streaming.news_basics where content_id = '{}';".format(content_id)
        select_query_result = self.read_from_mysql(statement)
        title = select_query_result[0][0]
        source = select_query_result[0][1]
        publish_time = select_query_result[0][2]
        return title, source, publish_time
    
    # news_basics_raw
    def get_news_content(self, content_id):
        statement = "SELECT content FROM streaming.news_basics_raw where publish_id = '{}';".format(content_id)
        select_query_result = self.read_from_mysql(statement)
        if select_query_result:
            return select_query_result[0][0]
        else:
            return '无内容'
    
    # news_cluster
    def get_cluster_id(self, content_id):
        cluster_id = -1
        statement = "SELECT cluster_id FROM streaming.news_cluster where content_id = '{}';".format(content_id)
        select_query_result = self.read_from_mysql(statement)
        for item in select_query_result:
            cluster_id = item[0]
        return cluster_id
    
    # news_publish_relation
    def get_repost_num(self, content_id):
        post_num = 0
        statement = "SELECT count(*) from streaming.news_publish_relation where content_id = '{}';".format(content_id)
        select_query_result = self.read_from_mysql(statement)
        for item in select_query_result:
            post_num = item[0]
        return post_num
    
    # special_related_event
    def from_special_to_event_id_list(self, special_id):
        statement = "SELECT event_id FROM melonfield.special_related_event WHERE special_id = '{}';".format(special_id)
        select_query_result = self.read_from_mysql(statement)
        event_id_list = list()
        for item in select_query_result:
            event_id_list.append(item[0])
        return event_id_list

    # important_event
    def check_important_event(self, event_id):
        statement = "SELECT event_type from melonfield.important_event where event_id = '{}';".format(event_id)
        select_query_result = self.read_from_mysql(statement)
        check = -1
        for item in select_query_result:
            check = item[0]
        return check
    
    def get_newly_add_important_event(self, gap=30):
        time_before = (datetime.now() - timedelta(minutes=gap)).strftime('%Y-%m-%d %H:%M:%S')
        statement = "SELECT event_id, event_type from melonfield.important_event where insert_time > '{}';".format(time_before)
        select_query_result = self.read_from_mysql(statement)
        event_dict = dict()
        for item in select_query_result:
            event_dict[item[0]] = item[1]
        return event_dict
    
    def get_history_news(self, event_id):
        statement = '''
            SELECT 
                etn1.content_id
            FROM
                melonfield.event_timeline_news etn1
                    LEFT JOIN
                melonfield.event_timeline_news etn2 
                ON etn1.event_id = etn2.event_id AND etn1.ts_in_min < etn2.ts_in_min
            		JOIN
            	streaming.news_basics nb
                ON etn1.content_id = nb.content_id
            WHERE
                etn1.event_id = {}
                    AND ISNULL(etn2.ts_in_min) order by publish_time;
        '''.format(event_id)
        select_query_result = self.read_from_mysql(statement)
        history_news = list()
        for i in select_query_result:
            history_news.append(i[0])
        return history_news
    
    def get_updated_news(self, event_id, start_time_str):
        statement = '''
            SELECT 
                ern.content_id
            FROM
                melonfield.event_related_news ern
            		JOIN
            	streaming.news_basics nb
                ON ern.content_id = nb.content_id
            WHERE
                ern.event_id = {}
                    AND nb.insert_time >'{}' order by nb.publish_time;
        '''.format(event_id, start_time_str)
        select_query_result = self.read_from_mysql(statement)
        history_news = list()
        for i in select_query_result:
            history_news.append(i[0])
        return history_news

    def get_latest_news_from_event(self, event_id, fetch_num=3):
        statement = '''
            SELECT 
                ern.content_id
            FROM
                melonfield.event_related_news ern
            		JOIN
            	streaming.news_basics nb
                ON ern.content_id = nb.content_id
            WHERE
                ern.event_id = {}
            ORDER BY nb.publish_time DESC
            LIMIT {};
        '''.format(event_id, fetch_num)
        select_query_result = self.read_from_mysql(statement)
        content_id_list = list()
        for i in select_query_result:
            content_id_list.append(i[0])
        return content_id_list
    