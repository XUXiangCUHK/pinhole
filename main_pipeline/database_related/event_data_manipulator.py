# -*- coding: utf-8 -*-
"""
Created on Tue Sep 29 16:20:50 2020

@author: user
"""
from database_related.database import database

class event_data_manipulator:
    def __init__(self):
        self.db = database()

    def get_all_event_info(self, start_time, timeline_potential_news_ts):
        event_info = self.get_all_event_basic(start_time)
        for event_id in event_info:
            entity_id_list = self.get_event_entity_id_list(event_id)
            event_info[event_id]['entity_id_list'] = entity_id_list
            news_id_list = self.get_event_news_id_list(event_id)
            event_info[event_id]['news_id_list'] = news_id_list
        
        important_event = self.get_important_event()
        for row in important_event:
            event_id = row[0]
            if event_info.get(event_id):
                event_type = row[1]
                direction_guide = row[2]
                history_timeline = self.get_event_history_timeline(event_id)
                latest_news = self.get_latest_news_from_event(event_id)
                timeline_potential_news = self.get_timeline_potenial_news(event_id, timeline_potential_news_ts)
                event_info[event_id]['event_type'] = event_type
                event_info[event_id]['direction_guide'] = direction_guide
                event_info[event_id]['history_timeline'] = history_timeline
                event_info[event_id]['timeline_potential_news'] = timeline_potential_news
                event_info[event_id]['latest_news'] = latest_news
        return event_info

    def get_all_event_basic(self, start_time):
#        ts_start = start_time.strftime('%Y%m%d%H%M')
        statement = "SELECT event_id, event_top_word, event_score FROM melonfield.event_latest_info where ts_in_min > '{}';".format(start_time)
        select_query_result = self.db.read_from_mysql(statement)
        event_info = dict()
        for row in select_query_result:
            event_id = row[0]
            event_top_word = eval(row[1])
            event_score = row[2]
            event_info[event_id] = {
                'event_top_word': event_top_word,
                'event_score': event_score
            }
        return event_info
    
    def get_event_entity_id_list(self, event_id):
        statement = "SELECT entity_id FROM melonfield.entity_related_event WHERE event_id = '{}';".format(event_id)
        select_query_result = self.db.read_from_mysql(statement)
        entity_id_list = list()
        for row in select_query_result:
            entity_id_list.append(row[0])
        return entity_id_list

    def get_event_news_id_list(self, event_id):
        statement = "SELECT content_id FROM melonfield.event_related_news WHERE event_id = '{}';".format(event_id)
        select_query_result = self.db.read_from_mysql(statement)
        news_collection = list()
        for item in select_query_result:
            news_collection.append(item[0])
        return news_collection

    def get_important_event(self):
        statement = "SELECT event_id, event_type, direction_guide FROM melonfield.important_event;"
        select_query_result = self.db.read_from_mysql(statement)
        return select_query_result
    
    def get_event_history_timeline(self, event_id):
        statement = """SELECT content_id FROM melonfield.event_timeline_news WHERE event_id = {}
        AND  ts_in_min = (SELECT MAX(ts_in_min) FROM melonfield.event_timeline_news WHERE event_id = {});""".format(event_id, event_id)
        select_query_result = self.db.read_from_mysql(statement)
        history_timeline = list()
        for row in select_query_result:
            history_timeline.append(row[0])
        return history_timeline        

    def get_timeline_potenial_news(self, event_id, timeline_potential_news_ts):
        statement = """SELECT ern.content_id FROM melonfield.event_related_news ern JOIN streaming.news_basics nb ON ern.content_id = nb.content_id 
        WHERE ern.event_id = {} AND ern.ts_in_min >= {} AND ern.ts_in_min <{} ORDER BY nb.publish_time;""".format(event_id, timeline_potential_news_ts[0], timeline_potential_news_ts[1])
        select_query_result = self.db.read_from_mysql(statement)
        news_collection = list()
        for item in select_query_result:
            news_collection.append(item[0])
        return news_collection        
        
    def get_initial_entity_id(self):
        statement = "SELECT max(entity_id) FROM melonfield.entity_info;"
        select_query_result = self.db.read_from_mysql(statement)
        if select_query_result:
            initial_entity_id = select_query_result[0][0]
            if initial_entity_id == None:
                return 0
            else:
                return initial_entity_id
        else:
            return 0    

    def get_initial_event_id(self):
        statement = "SELECT max(event_id) FROM melonfield.event_info;"
        select_query_result = self.db.read_from_mysql(statement)
        if select_query_result:
            initial_event_id = select_query_result[0][0]
            if initial_event_id == None:
                return 0
            else:
                return initial_event_id
        else:
            return 0  

    def insert_event_info(self, event_info, ts_in_min):
        for data in event_info:
            data['ts_in_min'] = ts_in_min
        statement = '''
                    INSERT IGNORE INTO `melonfield`.`event_info`
                    (`event_id`, `ts_in_min`, `news_num`, `event_score`, `event_top_word`, `abstract_title`, `abstract_content`)
                    VALUES
                    (%(event_id)s, %(ts_in_min)s, %(news_num)s, %(event_score)s, %(event_top_word)s, %(abstract_title)s, %(abstract_content)s);
                    '''
        self.db.write_into_mysql(event_info, statement)

    def insert_event_latest_info(self, event_info, ts_in_min):
        for data in event_info:
            data['ts_in_min'] = ts_in_min
        statement = '''
                    REPLACE INTO `melonfield`.`event_latest_info`
                    (`event_id`, `ts_in_min`, `news_num`, `event_score`, `event_top_word`, `abstract_title`, `abstract_content`)
                    VALUES
                    (%(event_id)s, %(ts_in_min)s, %(news_num)s, %(event_score)s, %(event_top_word)s, %(abstract_title)s, %(abstract_content)s);
                    '''
        self.db.write_into_mysql(event_info, statement)

    def insert_entity_related_event(self, entity_related_event, ts_in_min):
        for data in entity_related_event:
            data['ts_in_min'] = ts_in_min
        statement = '''
                    INSERT IGNORE INTO `melonfield`.`entity_related_event`
                    (`entity_id`, `event_id`, `ts_in_min`)
                    VALUES
                    (%(entity_id)s, %(event_id)s, %(ts_in_min)s);
                    '''
        self.db.write_into_mysql(entity_related_event, statement)
    
    def insert_event_timeline(self, event_timeline_insert, ts_in_min):
        for data in event_timeline_insert:
            data['ts_in_min'] = ts_in_min        
        statement = '''
                    INSERT IGNORE INTO `melonfield`.`event_timeline_news`
                    (`event_id`, `content_id`, `is_edited` , `ts_in_min`)
                    VALUES
                    (%(event_id)s, %(content_id)s, 0, %(ts_in_min)s);
                    '''
        self.db.write_into_mysql(event_timeline_insert, statement)        
        
    def get_latest_news_from_event(self, event_id):
        statement = '''
            SELECT 
                nm.content_id
            FROM
                melonfield.news_module nm
                JOIN
                streaming.news_basics nb ON nm.content_id = nb.content_id
            WHERE
                nm.news_module = 8 and nm.event_id = {} order by publish_time desc;
        '''.format(event_id)
        select_query_result = self.db.read_from_mysql(statement)
        content_id_list = list()
        for i in select_query_result:
            content_id_list.append(i[0])
        return content_id_list        
        