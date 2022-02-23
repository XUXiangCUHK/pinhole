# -*- coding: utf-8 -*-

# This file provides functions for tagging

from datetime import datetime
from collections import defaultdict


class tag:
    def __init__(self, min_overlap = 7):
        self.tag_min_overlap = min_overlap
    
    def tag_input_news(self, news_pool_tag, event_pool):
        update_event_id_list = list()
        event_related_news = list()
        event_updated_news = defaultdict(list)
        for event_id, event in event_pool.items():
            event_entity_id_list = event['entity_id_list']
            event_top_word = event['event_top_word']
            direction_guide = event.get('direction_guide', str())
            event_fixed = direction_guide.split(',') if direction_guide else list()

            event_top_word_dict = dict()
            for i in range(0, len(event_top_word)):
                event_top_word_dict[event_top_word[i]] = 10 / (i+1)

            for content_id, news in news_pool_tag.items():
                news_entity_id_list = news['entity_id_list']
                top_word_per_news = news['top_word_per_news']
                title = news['title']

                word_key_overlap = self.get_overlap(top_word_per_news, event_top_word[:6])
                if word_key_overlap < 2:
                    continue
                entity_overlap = self.get_overlap(news_entity_id_list, event_entity_id_list)
                word_overlap = self.get_overlap(top_word_per_news, event_top_word)
                if word_overlap + entity_overlap < 4:
                    continue
                
                score = 0
                for j in range(0, len(top_word_per_news)):
                    w = top_word_per_news[j]
                    if w in event_top_word:
                        score += event_top_word_dict[w]
                if event_fixed:
                    for x in event_fixed:
                        if x not in top_word_per_news:
                            score -= 2/len(event_fixed)
                        if x not in title:
                            score -= 2/len(event_fixed)
                
                exist_in_title = False
                for x in event_top_word[:6]:
                    if x in title:
                        exist_in_title = True
                        break

                if not exist_in_title:
                    continue

                if score > 14:
                    event_related_news.append({
                        'event_id': event_id, 
                        'content_id': content_id, 
                    })
                    update_event_id_list.append(event_id)
                    event_updated_news[event_id].append(content_id)
        print(event_related_news)            
        return list(set(update_event_id_list)), event_updated_news, event_related_news
    
    # def tag_input_news(self, news_pool_tag, event_pool):
    #     update_event_id_list = list()
    #     event_related_news = list()
    #     event_updated_news = defaultdict(list)
    #     for content_id, news in news_pool_tag.items():
    #         news_entity_id_list = news['entity_id_list']
    #         top_word_per_news = news['top_word_per_news']
    #         for event_id, event in event_pool.items():
    #             event_entity_id_list = event['entity_id_list']
    #             event_top_word = event['event_top_word']

    #             entity_overlap = self.get_overlap(news_entity_id_list, event_entity_id_list)
    #             word_overlap = self.get_overlap(top_word_per_news, event_top_word)
    #             if self.judge_standard(entity_overlap, word_overlap):
    #                 event_related_news.append({
    #                     'event_id': event_id, 
    #                     'content_id': content_id, 
    #                 })
    #                 update_event_id_list.append(event_id)
    #                 event_updated_news[event_id].append(content_id)
                    
    #     return list(set(update_event_id_list)), event_updated_news, event_related_news
    
    def judge_standard(self, entity_overlap, word_overlap):
        if word_overlap >= self.tag_min_overlap - entity_overlap:
            return True
        else:
            return False

    def get_overlap(self, list_a, list_b):
        total_list = list_a + list_b
        overlap = len(total_list) - len(set(total_list))
        return overlap