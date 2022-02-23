# -*- coding: utf-8 -*-

from datetime import datetime
from collections import Counter
from melon_modules.entity import entity
from melon_modules.abstract import abstract

class event_manipulator:
    def __init__(self, latest_event_id):
        self.entity = entity()
        self.abstract = abstract()
        self.latest_event_id = latest_event_id

    def find_new_event(self, news_cluster, max_news_num=100):
        new_event_cluster = list()
        for cluster_id, cluster_info in news_cluster.items():
            cluster_news_info = cluster_info['cluster_news_info']
            news_num = len(cluster_news_info)
            repost_num = cluster_info['repost_num']
            event_count_dict = cluster_info['event_count']
            max_event_count = sorted(event_count_dict.values(), reverse=True)[0]
            if max_event_count / news_num < 0.5 and news_num < max_news_num and repost_num > 10:
                self.latest_event_id += 1
                new_event_cluster.append([self.latest_event_id, cluster_id])
        return new_event_cluster
                
    def new_event_info_init(self, event_id, cluster_info, event_news_pool):
        news_id_list = list(event_news_pool.keys())
        event_related_news = self.get_event_related_news(event_id, news_id_list)
        event_entity, event_entity_id = self.entity.find_fixed_entity(news_id_list, percentage=0.5)
        entity_related_event = self.get_entity_related_event(event_entity_id, event_id)

        event_score = self.topic_evaluation(event_news_pool)
        event_top_word = self.get_event_top_word(event_news_pool)
        abstract_title = self.abstract.get_word_abstract_title(event_news_pool)
        abstract_content = self.abstract.get_sampling_abstract_content(event_news_pool)
        
        repost_num = cluster_info['repost_num']
        
        event_info = [{
            'event_id': event_id,
            'news_num': repost_num,
            'event_score': event_score,
            'event_top_word': str(event_top_word),
            'abstract_title': abstract_title,
            'abstract_content': abstract_content
        }]
        return event_related_news, entity_related_event, event_info

    def update_event(self, updated_event_id, event_news_pool):
        content_id_list = list(event_news_pool.keys())
        event_entity, event_entity_id = self.entity.find_fixed_entity(content_id_list, percentage=0.5)
        entity_related_event = self.get_entity_related_event(event_entity_id, updated_event_id)

        event_score = self.topic_evaluation(event_news_pool)
        event_top_word = self.get_event_top_word(event_news_pool)
        abstract_title = self.abstract.get_word_abstract_title(event_news_pool)        
        abstract_content = self.abstract.get_sampling_abstract_content(event_news_pool)
        
        news_num = 0
        for content_id, news_info in event_news_pool.items():
            news_num += news_info['repost_num']
        event_info = [{
                'event_id': updated_event_id,
                'news_num': news_num,
                'event_score': event_score,
                'event_top_word': str(event_top_word),
                'abstract_title': abstract_title,
                'abstract_content': abstract_content
            }]

        return entity_related_event, event_info

    def get_entity_related_event(self, entity_id_list, event_id):
        entity_related_event = list()
        for entity_id in entity_id_list:
            entity_related_event.append({
                'entity_id': entity_id,
                'event_id': event_id
            })
        return entity_related_event
    
    def get_event_related_news(self, event_id, news_id_list):
        event_related_news = list()
        for news_id in news_id_list:
            event_related_news.append({
                'event_id': event_id,
                'content_id': news_id
            })
        return event_related_news    

    def topic_evaluation(self, event_news_pool):
        entity_list = list()
        for news_id in event_news_pool:
            top_word_per_news = event_news_pool[news_id].get(news_id, dict()).get('top_word_per_news', list())
            entity_list += top_word_per_news
        event_score = (len(entity_list) - len(set(entity_list))) / len(event_news_pool)
        return event_score
    
    def get_event_top_word(self, event_news_pool):
        total_TF_count = Counter()
        for news_id in event_news_pool:
            tf_per_news = event_news_pool[news_id].get('tf_per_news', Counter())
            total_TF_count += tf_per_news
        event_top_word = self.get_top_entity(total_TF_count, 15)
        return event_top_word
    
    def get_top_entity(self, counter, most_common=15):
        top_entity_list = [k for (k,v) in list(counter.most_common(most_common))]
        return top_entity_list
    
    

    