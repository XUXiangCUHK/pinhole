# -*- coding: utf-8 -*-
"""
Created on Tue Sep 29 16:20:50 2020

@author: user
"""
from database_related.database import database
from collections import defaultdict, Counter

class news_data_manipulator:
    def __init__(self):
        self.db = database()
    
    def get_tag_news_info(self, start_time_str, end_time_str):
        statement = '''
                SELECT
                    a.content_id, a.top_word_per_news, b.title
                FROM
                    melonfield.filtered_news_info a
                JOIN
                    streaming.news_basics b ON a.content_id = b.content_id
                WHERE
                    b.insert_time >= '{}' AND b.insert_time < '{}';
                '''.format(start_time_str, end_time_str)
        select_query_result = self.db.read_from_mysql(statement)    
        tag_news_pool = dict()
        for item in select_query_result:
            content_id = item[0]
            top_word_per_news = eval(item[1])
            title = item[2]
            entity_id_list = self.get_news_entity_id_list(content_id)
            
            tag_news_pool[content_id] = {
                'entity_id_list': entity_id_list,
                'top_word_per_news': top_word_per_news,
                'title': title
            }
        return tag_news_pool

    def get_news_entity_id_list(self, content_id):
        statement = "SELECT entity_id FROM melonfield.filtered_news_entity WHERE content_id = '{}';".format(content_id)
        select_query_result = self.db.read_from_mysql(statement)
        news_entity_id_list = list()
        for item in select_query_result:
            news_entity_id_list.append(item[0])
        return news_entity_id_list

    def get_event_news_info(self, event_id):
        statement = '''
              SELECT
        			fni.publish_time, fni.content_id, fni.tf_per_news, fni.top_word_per_news, npr.segged_title, nbr.title, nbr.content, fni.repost_num, nv.word2vec, nb.source_id, nb.platform
        		FROM
        			melonfield.filtered_news_info fni
        		JOIN
        			melonfield.event_related_news ern ON fni.content_id = ern.content_id
        		JOIN
        			melonfield.news_preprocess_result npr ON fni.content_id = npr.content_id
        		JOIN
        			streaming.news_basics_raw nbr ON fni.content_id = nbr.publish_id
              JOIN
                 streaming.news_basics nb ON fni.content_id = nb.content_id
        		JOIN
        			streaming.news_word2vec nv ON fni.content_id = nv.content_id
        		WHERE
        			ern.event_id = {} order by publish_time;
                    '''.format(event_id)
        event_news_pool = dict()
        select_query_result = self.db.read_from_mysql(statement)
        for item in select_query_result:
            publish_time = item[0]
            content_id = item[1]
            tf_per_news = eval(item[2])
            top_word_per_news = eval(item[3])
            segged_title = eval(item[4])
            title = item[5]
            content = item[6]
            repost_num = item[7]
            word2vec = item[8].split(',')
            source_id = item[9]
            platform = item[10]
            c = 0
            corpus = list()
            for i in word2vec:
                corpus.append((c,float(i)))
                c+=1
                
            event_news_pool[content_id] = {
                'publish_time': publish_time,
                'tf_per_news': tf_per_news,
                'top_word_per_news': top_word_per_news,
                'segged_title': segged_title,
                'title': title,
                'content': content,
                'repost_num': repost_num,
                'timestamp': int(publish_time.timestamp()),
                'corpus' : corpus,
                'source_id' : source_id,
                'platform' : platform
            }
        return event_news_pool
        
    def get_news_pool(self, event_id_list):
        news_pool = dict()
        for event_id in event_id_list:
            event_news_pool = self.get_event_news_info(event_id)
            news_pool.update(event_news_pool)
        return news_pool
    
    def get_single_news_info(self, content_id):
        statement = '''
            SELECT
                a.publish_time, a.content_id, a.tf_per_news, a.top_word_per_news, b.segged_title, r.title, r.content, a.repost_num, nv.word2vec, nb.source_id, nb.platform
            FROM
                melonfield.filtered_news_info a
            JOIN
                melonfield.news_preprocess_result b ON a.content_id = b.content_id
            JOIN
                streaming.news_basics_raw r ON a.content_id = r.publish_id
            JOIN
                streaming.news_basics nb ON a.content_id = nb.content_id
            JOIN
                streaming.news_word2vec nv ON a.content_id = nv.content_id
            WHERE
                a.content_id = '{}';
            '''.format(content_id)
        select_query_result = self.db.read_from_mysql(statement)    
        news_info = dict()
        for item in select_query_result:
            publish_time = item[0]
            content_id = item[1]
            tf_per_news = eval(item[2])
            top_word_per_news = eval(item[3])
            segged_title = eval(item[4])
            title = item[5]
            content = item[6]
            repost_num = item[7]
            word2vec = item[8].split(',')
            source_id = item[9]
            platform = item[10]
            c = 0
            corpus = list()
            for i in word2vec:
                corpus.append((c,float(i)))
                c+=1
            news_info = {content_id : {
                'publish_time': publish_time,
                'tf_per_news': tf_per_news,
                'top_word_per_news': top_word_per_news,
                'segged_title': segged_title,
                'title': title,
                'content': content,
                'repost_num': repost_num,
                'timestamp': int(publish_time.timestamp()),
                'corpus' : corpus,
                'source_id' : source_id,
                'platform' : platform
            }}
        return news_info

    def get_news_info_from_list(self, content_id_list):
        news_pool = dict()
        for content_id in content_id_list:
            news_info = self.get_single_news_info(content_id)
            news_pool.update(news_info)
        return news_pool
            
    def get_important_event_news_pool(self, event_info, updated_event_id):
        news_pool = dict()
        event_id_set = set()
        for event_id, event_dict in event_info.items():
            event_type = event_info[event_id].get('event_type', -1)
            if event_type>=0 and event_id in updated_event_id:
                event_news_pool = self.get_event_news_info(event_id)
                news_pool.update(event_news_pool)
                event_id_set.add(event_id)
        for event_id, event_dict in event_info.items():
            timeline_potential_news = event_dict.get('timeline_potential_news',list())
            if len(timeline_potential_news)>0 and event_id not in event_id_set:
                event_news_pool = self.get_event_news_info(event_id)
                news_pool.update(event_news_pool)
                event_id_set.add(event_id)
        return news_pool

    def get_news_cluster(self, start_time_24h, end_time_24h, publish_time_48h, cluster_limit = 3):
        statement = '''
                SELECT
                    f.content_id, cluster_id, event_id, f.repost_num, npr.segged_title
                FROM
                    melonfield.filtered_news_info f
                JOIN
	                    streaming.news_basics nb ON f.content_id = nb.content_id
                JOIN
                    streaming.news_cluster c ON f.content_id = c.content_id
				JOIN
					melonfield.news_preprocess_result npr on f.content_id = npr.content_id
                LEFT JOIN
                    melonfield.event_related_news e ON f.content_id = e.content_id
                WHERE
                    nb.insert_time >= '{}' and nb.insert_time < '{}' and nb.publish_time >= '{}'
                    AND 
                    c.cluster_id in
                        (
                        SELECT
                            c.cluster_id
                        FROM
                            melonfield.filtered_news_info f
                        JOIN
			                    streaming.news_basics nb ON f.content_id = nb.content_id
                        JOIN
                            streaming.news_cluster c ON f.content_id = c.content_id
                        WHERE
                            nb.insert_time >= '{}' and nb.insert_time < '{}' and nb.publish_time >= '{}'
                        GROUP BY 
                            c.cluster_id 
                        HAVING 
                            count(*) >= {}
                        ) order by nb.publish_time;
                    '''.format(start_time_24h, end_time_24h, publish_time_48h, start_time_24h, end_time_24h, publish_time_48h, cluster_limit)
        select_query_result = self.db.read_from_mysql(statement)
        news_cluster = dict()
        for item in select_query_result:
            content_id = item[0]
            cluster_id = item[1]
            event_id = item[2]
            repost_num = item[3]
            segged_title = eval(item[4])
            
            event_count = 0
            if event_id:
                event_count = 1

            if news_cluster.get(cluster_id):
                news_cluster[cluster_id]['cluster_news_info'][content_id] ={
                                                    'segged_title': segged_title,
                                                    'repost_num': repost_num
                                                }
                news_cluster[cluster_id]['event_count'][event_id] += event_count
                news_cluster[cluster_id]['repost_num'] += repost_num
            else:
                event_count_dict = defaultdict(int)
                event_count_dict[event_id] += event_count
                news_cluster[cluster_id] = {
                    'cluster_news_info': {content_id: {'repost_num': repost_num, 'segged_title': segged_title}}, 
                    'event_count': event_count_dict, 
                    'repost_num': repost_num
                }
        return news_cluster        
    
    def insert_news_abstract(self, news_abstract_list):
        statement = '''
                    REPLACE INTO `melonfield`.`news_abstract`
                    (`content_id`, `title`, `short_abstract`, `abstract`, `highlight`)
                    VALUES
                    (%(content_id)s, %(title)s, %(short_abstract)s, %(abstract)s, %(highlight)s);
                    '''
        self.db.write_into_mysql(news_abstract_list, statement)

    def insert_event_related_news(self, event_related_news, ts_in_min):
        for data in event_related_news:
            data['ts_in_min'] = ts_in_min
        statement = '''
                    INSERT IGNORE INTO `melonfield`.`event_related_news`
                    (`event_id`, `content_id`, `ts_in_min`)
                    VALUES
                    (%(event_id)s, %(content_id)s, %(ts_in_min)s);
                    '''
        self.db.write_into_mysql(event_related_news, statement)

    def insert_recent_hot_news(self, recent_hot_news, ts_in_min):
        for data in recent_hot_news:
            data['ts_in_min'] = ts_in_min
        statement = '''
                    INSERT IGNORE INTO `melonfield`.`recent_hot_news`
                    (`content_id`, `hotness`, `abstract_title`, `ts_in_min`)
                    VALUES
                    (%(content_id)s, %(hotness)s, %(abstract_title)s, %(ts_in_min)s);
                    '''
        self.db.write_into_mysql(recent_hot_news, statement)

    def insert_latest_news_into_news_module(self, event_latest_news):
        statement = '''
                    INSERT IGNORE INTO `melonfield`.`news_module`
                    (`event_id`, `content_id`, `news_module`)
                    VALUES
                    (%(event_id)s, %(content_id)s, %(news_module)s);
                    '''
        self.db.write_into_mysql(event_latest_news, statement)
    
    def delete_from_news_module(self, delete_info_list):
        statement = "DELETE FROM melonfield.news_module where event_id = %s and news_module = %s;"
        self.db.write_into_mysql(delete_info_list, statement)
