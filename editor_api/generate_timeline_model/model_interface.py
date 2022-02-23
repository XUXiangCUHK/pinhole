# -*- coding: utf-8 -*-
from datetime import datetime,timedelta
import requests
from event_generate.story_timeline_manipulator import story_timeline_manipulator
from collections import Counter

class load_model():
    def __init__(self):
        self.model_name = 'generate_timeline_model'
        self.model_version = '1.0'
        self.stm = story_timeline_manipulator()

    def process(self, input_json):
        event_id = input_json.get('event_id')
        event_type = input_json.get('event_type')

        status = self.stm.regenerate_story_timeline(event_id, event_type)
        
        output_dict = dict()
        output_dict['status'] = status
        return output_dict

    def get_version(self):
        return self.model_version

    def get_name(self):
        return self.model_name

    def svc(self,url, input_dict):
        response = requests.post(url=url, json=input_dict)
        return response.json() if response.ok else False
