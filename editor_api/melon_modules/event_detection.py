# -*- coding: utf-8 -*-

from datetime import datetime
from collections import Counter
from models.database import Database
from models.leaf import Leaf
from models.entity import Entity
from models.abstract_gen import Abstract
from models.news_pool_V2 import News_pool

class Event_detection:
    def __init__(self):
        self.database = Database()
        self.leaf = Leaf()
        self.entity = Entity()
        self.abstract = Abstract()
        self.news_pool = News_pool()

        self.event_id_num = self.database.get_initial_event_id()
    
    def find_new_event(self, news_cluster_pool, ts_in_min, max_news_num=100):
        event_pool = list()
        flag = 0
        for cluster_id, news_cluster in news_cluster_pool.items():
            news_num = len(news_cluster['news_collection'])
            repost_num = news_cluster['repost_num']
            if news_num == 0:
                return -2
            if news_cluster['event_count'] / news_num < 0.5 and news_num < max_news_num and repost_num > 10:
                flag = 1
                self.event_id_num += 1
                try:
                    news_collection = list(news_cluster['news_collection'].keys())
                except:
                    news_collection = news_cluster['news_collection']
                self.add_event_related_news(self.event_id_num, news_collection, ts_in_min)
                news_pool = self.news_pool.create_news_pool(news_collection)

                event_entity, event_entity_id = self.entity.find_fixed_entity(news_collection, percentage=0.5)
                self.add_entity_related_event(event_entity_id, self.event_id_num, ts_in_min)

                event_score = self.topic_evaluation(news_collection, news_pool)
                event_top_word = self.get_event_top_word(news_collection, news_pool)
                abstract_title = self.abstract.get_abstract_title(news_collection, news_pool)
                
                leaf_content_id_list = self.get_leaf(news_collection, news_pool)
                abstract_content = self.abstract.get_abstract_content(leaf_content_id_list, news_pool)
                # self.abstract.make_abstract_for_key_news_list(leaf_content_id_list, news_pool)

                event_pool.append({
                    'event_id': self.event_id_num,
                    'ts_in_min': ts_in_min,
                    'news_num': repost_num,
                    'event_score': event_score,
                    'event_top_word': str(event_top_word),
                    'abstract_title': abstract_title,
                    'abstract_content': abstract_content,
                    'leaf_content_id_list': str(leaf_content_id_list)
                })
                
                # print(self.event_id_num, news_num, event_score)
                # for news_id in news_collection:
                #     title, _, _ = self.database.get_news_info(news_id)
                #     print(title)
                # print(event_top_word)
                # print(abstract_title)
                # print(abstract_content)

        self.database.insert_event_info(event_pool)
        self.database.insert_event_latest_info(event_pool)
        if flag == 1:
            return self.event_id_num
        else:
            return -1
    
    def add_entity_related_event(self, entity_id_list, event_id, ts_in_min):
        entity_related_event = list()
        for entity_id in entity_id_list:
            entity_related_event.append({
                'entity_id': entity_id,
                'event_id': event_id,
                'ts_in_min': ts_in_min
            })
        self.database.insert_entity_related_event(entity_related_event)
    
    def add_event_related_news(self, event_id, news_collection, ts_in_min):
        event_related_news = list()
        for news_id in news_collection:
            event_related_news.append({
                'event_id': event_id,
                'content_id': news_id,
                'ts_in_min': ts_in_min
            })
        self.database.insert_event_related_news(event_related_news)

    def topic_evaluation(self, news_collection, news_pool):
        entity_list = list()
        for news_id in news_collection:
            top_word_per_news = news_pool.get(news_id, dict()).get('top_word_per_news', list())
            if not top_word_per_news:
                _, top_word_per_news = self.database.get_news_attribution(news_id)
            entity_list += top_word_per_news
        event_score = (len(entity_list) - len(set(entity_list))) / len(news_collection)
        return event_score
    
    def get_event_top_word(self, news_collection, news_pool):
        total_TF_count = Counter()
        for news_id in news_collection:
            tf_per_news = news_pool.get(news_id, dict()).get('tf_per_news', Counter())
            if not tf_per_news:
                tf_per_news, _ = self.database.get_news_attribution(news_id)
            total_TF_count += tf_per_news
        event_top_word = self.get_top_entity(total_TF_count, 15)
        return event_top_word
    
    def get_top_entity(self, counter, most_common=15):
        top_entity_list = [k for (k,v) in list(counter.most_common(most_common))]
        return top_entity_list
    
    # def get_abstract_title(self, event_top_word):
    #     length = min(4, len(event_top_word))
    #     return ' '.join(event_top_word[0:length])
    
    # def get_abstract_content(self, news_collection, news_pool):
    #     news_id = news_collection[0]
    #     content = news_pool.get(news_id, dict()).get('content', str())
    #     if not content:
    #         content = self.database.get_news_content(news_id)
    #     content_split = content.split('。')
    #     length = min(3, len(content_split))
    #     return ' '.join(content_split[0:length])
    
    def get_leaf(self, news_collection, news_pool):
        if len(news_collection) <=5:
            return news_collection
        else:
            return self.leaf.make_leaf_for_news_collection(news_collection, news_pool)