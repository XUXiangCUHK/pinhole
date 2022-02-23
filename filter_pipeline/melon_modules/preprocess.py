# -*- coding: utf-8 -*-

# This file provides preprocess functions inculding seg and TF count

import requests
from collections import Counter

class Preprocess:
    def __init__(self):
        self.preprocess_api_url = 'http://47.106.46.242:30001/calculate'
        self.stopwords = self.get_stop_words(filename='./melon_modules/stop_words.txt')
        self.finance_sw = self.get_stop_words(filename='./melon_modules/finance_stop_words.txt')
        self.most_common = 15
    
    def get_stop_words(self, filename):
        with open(filename, 'rb') as f:
            stopWords = [line.strip().decode('utf-8') for line in f.readlines()]
        return stopWords

    def preprocess_api(self, title, content):
        r = requests.post(
            self.preprocess_api_url,
            json={
                'model_name': 'preprocess',
                'model_version': '2.0',
                'title': title,
                'content': content,
                'extra_args': list()
                }
        )
        response_dict = r.json()
        return response_dict['segged_title'], response_dict['segged_content']

    def words_filter(self, segged_words, take_noun=False, rm_sw=True, rw_fsw=True):
        stopwords_in = self.stopwords if rm_sw else list()
        finance_sw_in = self.finance_sw if rw_fsw else list()
        all_sw = stopwords_in + finance_sw_in
        if take_noun:
            filtered_list = [(x[0],x[1]) for x in segged_words if x[1].startswith('n') and len(x[0])>1 and x[0] not in all_sw]
        else:
            filtered_list = [(x[0],x[1]) for x in segged_words if (x[1].startswith('n') or x[1].startswith('v')) and len(x[0])>1 and x[0] not in all_sw]
        return filtered_list

    def count_TF(self, all_words, most_common, normalize=True):
        c = Counter(all_words)
        if normalize:
            for k, v in c.items():
                c[k] = v / c.most_common(most_common)[-1][1]
        return c

    def get_top_words_list(self, counter, most_common):
        top_words_list = [k for (k,v) in list(counter.most_common(most_common))]
        return top_words_list