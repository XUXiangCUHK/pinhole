# -*- coding: utf-8 -*-

# This file provides functions related with entity detection

from LAC import LAC
from collections import Counter
from melon_modules.database import Database

class Entity:
    def __init__(self):
        self.lac = LAC(mode='lac')
        self.database = Database()
        self.useful_tag_list = ['ORG', 'PER', 'LOC', 'nz', 'nw']
        self.entity_id_number = self.database.get_initial_entity_id()

    def get_entities_list(self, title):
        title_lac_result = self.lac.run([title])
        for result in title_lac_result:
            title_entity_list = list()
            for i in range(0, len(result[1])):
                if result[1][i] in self.useful_tag_list:
                    title_entity_list.append((result[0][i], result[1][i]))
        return title_entity_list
    
    def check_entity_status(self, entity):
        entity_name = entity[0]
        entity_type = entity[1]
        entity_name = entity_name.strip()
        entity_id = self.database.from_entity_name_to_id(entity_name)
        if entity_id >= 0:
            return entity_id
        else:
            self.entity_id_number += 1
            entity_id = self.entity_id_number
            self.update_entity_info(entity_id, entity_name, entity_type)
            return entity_id
    
    def update_entity_info(self, entity_id, entity_name, entity_type):
        data = [{'entity_id': entity_id, 'entity_name': entity_name, 'entity_type': entity_type}]
        self.database.insert_entity_info_json(data)