# -*- coding: utf-8 -*-
from datetime import datetime,timedelta

import requests
import json

class load_model():
    def __init__(self):
        self.model_name = 'key_word_search_model'
        self.model_version = '1.0'
        
    def process(self, input_json):
        key_word = input_json.get('key_word', None)
        
        event_info = self.get_event_info(key_word)
        
        output_dict = dict()
        output_dict['event_info'] = event_info
        return output_dict

    def get_version(self):
        return self.model_version

    def get_name(self):
        return self.model_name
    
    def svc(self, url, input_dict):
        response = requests.post(url=url, json=input_dict)
        return response.json() if response.ok else False

    def get_event_info(self,key_word):
        # try:
        edit_url = 'http://localhost:40000/calculate'
        input_dict = {
                'model_name': 'frontpage_search_model',
                'model_version': '1.0',
                'key_words': key_word
                }
        r = self.svc(edit_url,input_dict)
        return r['search_result']
        # except:
        #     return dict()
            




        
        
        
        
        