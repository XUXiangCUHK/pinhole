# -*- coding: utf-8 -*-

# This file provides functions related with entity detection

from LAC import LAC
from collections import Counter
from models.database import Database

class Entity:
    def __init__(self):
        self.lac = LAC(mode='lac')
        self.database = Database()
        self.useful_tag_list = ['ORG', 'PER', 'LOC', 'nz', 'nw']
        self.entity_id_number = self.database.get_initial_entity_id()

    def get_entities_list(self, title):
        title_lac_result = self.lac.run([title])
        # print('title_lac_result: ', title_lac_result)
        for result in title_lac_result:
            title_entity_list = list()
            for i in range(0, len(result[1])):
                if result[1][i] in self.useful_tag_list:
                    title_entity_list.append(result[0][i])
        return title_entity_list
    
    def check_entity_status(self, entity_name):
        entity_name = entity_name.strip()
        entity_id = self.database.from_entity_name_to_id(entity_name)
        if entity_id >= 0:
            return entity_id
        else:
            self.entity_id_number += 1
            entity_id = self.entity_id_number
            self.update_entity_info(entity_id, entity_name)
            return entity_id
    
    def update_entity_info(self, entity_id, entity_name):
        data = [{'entity_id': entity_id, 'entity_name': entity_name}]
        self.database.insert_entity_info(data)
    
    # find common entity in event
    def find_fixed_entity(self, news_collection, percentage = 0.5):
        target = list()
        target_id = list()
        TF_entity_per_topic = Counter()
        TF_entity_id_per_topic = Counter()
        length = len(news_collection)

        for news_id in news_collection:
            news_entity_list = self.database.get_news_entity_list(news_id)
            news_entity_id_list = self.database.get_news_entity_id_list(news_id)
            TF_entity_per_topic += Counter(news_entity_list)
            TF_entity_id_per_topic += Counter(news_entity_id_list)

        for (k,v) in list(TF_entity_per_topic.most_common(5)):
            if v / length > percentage:
                target.append(k)
        for (k,v) in list(TF_entity_id_per_topic.most_common(5)):
            if v / length > percentage:
                target_id.append(k)
        return target, target_id