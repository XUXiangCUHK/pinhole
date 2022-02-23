# -*- coding: utf-8 -*-

import json
from datetime import datetime, timedelta
from collections import Counter
from models.database import Database

class News_pool_tag:
    def __init__(self, start_time_str, end_time_str):
        self.database = Database()
        self.start_time_str = start_time_str
        self.end_time_str = end_time_str
    
    def fetch_from_database_tag(self, gap):
        statement = '''
                    SELECT
                        a.content_id, a.top_word_per_news
                    FROM
                        melonfield.filtered_news_info a
                    JOIN
                        streaming.news_basics b ON a.content_id = b.content_id
                    WHERE
                        b.insert_time >= '{}' AND b.insert_time < '{}';
                    '''.format(self.start_time_str, self.end_time_str)
        select_query_result = self.database.read_from_mysql(statement)
        return select_query_result
    
    def create_news_pool_tag(self, gap=30):
        select_query_result = self.fetch_from_database_tag(gap)
        news_pool = dict()
        for item in select_query_result:
            content_id = item[0]
            top_word_per_news = eval(item[1])
            entity_id_list = self.database.get_news_entity_id_list(content_id)
            
            news_pool[content_id] = {
                'entity_id_list': entity_id_list,
                'top_word_per_news': top_word_per_news,
            }
        return news_pool
    
class News_pool:
    def __init__(self):
        self.database = Database()
        
    def fetch_from_database(self, content_id):
        statement = '''
                    SELECT
                        a.publish_time, a.content_id, a.tf_per_news, a.top_word_per_news, b.segged_title, r.title, r.content, a.repost_num, nv.word2vec
                    FROM
                        melonfield.filtered_news_info a
                    JOIN
                        melonfield.news_preprocess_result b ON a.content_id = b.content_id
                    JOIN
                        streaming.news_basics_raw r ON a.content_id = r.publish_id
                    JOIN
                        streaming.news_word2vec nv ON a.content_id = nv.content_id
                    WHERE
                        a.content_id = '{}';
                    '''.format(content_id)
        select_query_result = self.database.read_from_mysql(statement)
        return select_query_result

    def create_news_pool(self, news_collection):
        news_pool = dict()
        for news_id in news_collection:
            select_query_result = self.fetch_from_database(news_id)
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
                c = 0
                corpus = list()
                for i in word2vec:
                    corpus.append((c,float(i)))
                    c+=1
                    
                news_pool[content_id] = {
                    'publish_time': publish_time,
                    'tf_per_news': tf_per_news,
                    'top_word_per_news': top_word_per_news,
                    'segged_title': segged_title,
                    'title': title,
                    'content': content,
                    'repost_num': repost_num,
                    'timestamp': int(publish_time.timestamp()),
                    'corpus' : corpus
                }
        return news_pool