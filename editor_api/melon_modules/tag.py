# -*- coding: utf-8 -*-

# This file provides functions for tagging

from datetime import datetime
from models.database import Database

class Tag:
    def __init__(self, min_overlap, ts_in_min):
        self.database = Database()
        self.tag_min_overlap = min_overlap
        self.ts_in_min = ts_in_min
    
    def tag_input_news(self, news_pool_tag, event_pool):
        update_event_id_list = list()
        event_related_news = list()
        for content_id, news in news_pool_tag.items():
            news_entity_id_list = news['entity_id_list']
            top_word_per_news = news['top_word_per_news']
            for event_id, event in event_pool.items():
                event_entity_id_list = event['entity_id_list']
                event_top_word = event['event_top_word']

                entity_overlap = self.get_overlap(news_entity_id_list, event_entity_id_list)
                word_overlap = self.get_overlap(top_word_per_news, event_top_word)

                if self.judge_standard(entity_overlap, word_overlap):
                    event_related_news.append({
                        'event_id': event_id, 
                        'content_id': content_id, 
                        'ts_in_min': self.ts_in_min
                    })
                    update_event_id_list.append(event_id)
        self.database.insert_event_related_news(event_related_news)
        return list(set(update_event_id_list))
    
    def judge_standard(self, entity_overlap, word_overlap):
        if word_overlap >= self.tag_min_overlap - entity_overlap:
            return True
        else:
            return False

    def get_overlap(self, list_a, list_b):
        total_list = list_a + list_b
        overlap = len(total_list) - len(set(total_list))
        return overlap
    
    def tag_input_news_iterative(self, news_pool_tag, event_pool):
        update_event_id_list = list()
        event_related_news = list()
        for content_id, news in news_pool_tag.items():
            news_entity_id_list = news['entity_id_list']
            top_word_per_news = news['top_word_per_news']
            for event_id, event in event_pool.items():
                event_entity_id_list = event['entity_id_list']
                event_top_word = event['event_top_word']
                event_top_word_dict = dict()
                for i in range(0, len(event_top_word)):
                    event_top_word_dict[event_top_word[i]] = 10 / (i+1)
                
                score = 0
                for j in range(0, len(top_word_per_news)):
                    w = top_word_per_news[j]
                    if w in event_top_word:
                        score += event_top_word_dict[w]

                entity_overlap = self.get_overlap(news_entity_id_list, event_entity_id_list)
                word_overlap = self.get_overlap(top_word_per_news, event_top_word)
                word_key_overlap = self.get_overlap(top_word_per_news, event_top_word[:6])

                if word_key_overlap >= 3 and (word_overlap + entity_overlap) >= 5 and score > 18:
                    event_related_news.append({
                        'event_id': event_id, 
                        'content_id': content_id, 
                        'ts_in_min': self.ts_in_min
                    })
                    update_event_id_list.append(event_id)
        self.database.insert_event_related_news(event_related_news)
        return list(set(update_event_id_list))