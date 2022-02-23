# -*- coding: utf-8 -*-

from datetime import datetime, timedelta
from collections import Counter
from melon_modules.entity import Entity
from melon_modules.abstract import Abstract
from melon_modules.database import Database
from melon_modules.news_pool_V2 import News_pool
from melon_modules.event_detection import Event_detection

class Update:
    def __init__(self):
        self.entity = Entity()
        self.abstract = Abstract()
        self.database = Database()
        self.news_pool = News_pool()
        self.event_detection = Event_detection()
    
    def update_event(self, ts_before, ts_in_min):
        updated_event = list()
        event_id_list = self.database.get_recent_event_id(ts_before)
        for event_id in event_id_list:
            news_collection = self.database.from_event_to_news_collection(event_id)
            news_pool = self.news_pool.create_news_pool(news_collection)
            news_num = len(news_collection)

            event_entity, event_entity_id = self.entity.find_fixed_entity(news_collection, percentage=0.5)
            self.event_detection.add_entity_related_event(event_entity_id, event_id, ts_in_min)

            event_score = self.event_detection.topic_evaluation(news_collection, news_pool)
            event_top_word = self.event_detection.get_event_top_word(news_collection, news_pool)
            abstract_title = self.abstract.get_abstract_title(news_collection, news_pool)        
            abstract_content = self.abstract.get_abstract_content(leaf_content_id_list, news_pool)

            updated_event.append({
                    'event_id': event_id,
                    'ts_in_min': ts_in_min,
                    'news_num': news_num,
                    'event_score': event_score,
                    'event_top_word': str(event_top_word),
                    'abstract_title': abstract_title,
                    'abstract_content': abstract_content
                })

        self.database.insert_event_info(updated_event)
        self.database.insert_event_latest_info(updated_event)