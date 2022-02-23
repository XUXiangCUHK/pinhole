# -*- coding: utf-8 -*-
import requests
from collections import Counter, defaultdict
import time
from urllib.parse import urlencode
from urllib.request import urlopen
from urllib.error import URLError
from urllib.request import Request
import json
import random
from textrank4zh import TextRank4Keyword, TextRank4Sentence
import re

class abstract:
    def __init__(self):
        api_key = 'Po5IwLHcVdvalD3mtb0F8cKG'
        secret_key = 'zkN29vY0E4vW6ZIXEiNxYDcdD6z8Gslh'
        token = self.fetch_token(api_key, secret_key)
        SUMMARY_URL = "https://aip.baidubce.com/rpc/2.0/nlp/v1/news_summary"
        self.summary_url = SUMMARY_URL + "?charset=UTF-8&access_token=" + token
        self.tr4s = TextRank4Sentence()

    def fetch_token(self, API_KEY, SECRET_KEY):
        TOKEN_URL = 'https://aip.baidubce.com/oauth/2.0/token'
        params = {'grant_type': 'client_credentials',
                  'client_id': API_KEY,
                  'client_secret': SECRET_KEY}
        post_data = urlencode(params)
        post_data = post_data.encode('utf-8')
        req = Request(TOKEN_URL, post_data)
        try:
            f = urlopen(req, timeout=5)
            result_str = f.read()
        except URLError as err:
            print(err)
        result_str = result_str.decode()
        result = json.loads(result_str)
        if ('access_token' in result.keys() and 'scope' in result.keys()):
            if not 'brain_all_scope' in result['scope'].split(' '):
                print ('please ensure has check the  ability')
                exit()
            return result['access_token']
        else:
            print ('please overwrite the correct API_KEY and SECRET_KEY')
            exit()
    
    def clean_content(self, content):
         clean_code = re.compile(r'[a-zA-Z}{)(=; :\/_\"\'.0-9\\|!,&]{20,}')
         content = clean_code.sub('',content)
         return content
    
    def get_news_abstract(self, title, content, max_summary_len=50):
        data = {'title': title, 'content': content[:2900], 'max_summary_len': max_summary_len}
        result = requests.post(self.summary_url, data=json.dumps(data)).json()
        # 防止qps超限
        time.sleep(0.5)
        return result.get('summary')

    def get_highlight_sent(self, text):
        if text:
            self.tr4s.analyze(text=text, lower=True, source = 'all_filters')

            for item in self.tr4s.get_key_sentences(num=2):
                if item.index == 0:
                    continue
                else:
                    return item.sentence
        else:
            return ''

    def get_event_latest_valid_news_abstract(self, event_latest_news, event_updated_news, news_pool):
        timeline_abstract_list = list()
        for data in event_latest_news:
            event_id = data['event_id']
            content_id = data['content_id']
            if content_id in event_updated_news[event_id]:
                title = news_pool[content_id]['title']
                content = news_pool[content_id]['content']
                content = self.clean_content(content)
                short_abstract = self.get_news_abstract(title, content)
                abstract = self.get_news_abstract(title, content, 150)
                highlight = self.get_highlight_sent(abstract)
                abstract_dict = {
                        'content_id': content_id,
                        'title': title,
                        'short_abstract': short_abstract,
                        'abstract': abstract,
                        'highlight': highlight
                        }
                
                timeline_abstract_list.append(abstract_dict)
        return timeline_abstract_list

    def get_event_updated_timeline_abstract(self, updated_timeline, history_timeline, news_pool):
        timeline_abstract_list = list()
        history_timeline_set = set(history_timeline)
        for content_id in updated_timeline:
            if content_id not in history_timeline_set:
                title = news_pool[content_id]['title']
                content = news_pool[content_id]['content']
                content = self.clean_content(content)
                short_abstract = self.get_news_abstract(title, content)
                abstract = self.get_news_abstract(title, content, 300)
                highlight = self.get_highlight_sent(abstract)
                abstract_dict = {
                        'content_id': content_id,
                        'title': title,
                        'short_abstract': short_abstract,
                        'abstract': abstract,
                        'highlight': highlight
                        }
                
                timeline_abstract_list.append(abstract_dict)
        return timeline_abstract_list
    
    def get_updated_important_event_abstract(self, event_info, news_pool):
        news_abstract_list = list()
        for event_id, event_dict in event_info.items():
            updated_timeline = event_dict.get('updated_timeline')
            if updated_timeline:
                history_timeline = event_dict.get('history_timeline')
                timeline_abstract_list = self.get_event_updated_timeline_abstract(updated_timeline, history_timeline, news_pool)
                news_abstract_list += timeline_abstract_list
        return news_abstract_list
    
    def get_word_abstract_title(self, news_pool):
        total_title_counter = Counter()
        for news_id, news_info in news_pool.items():
            segged_title = news_info['segged_title']
            tf_title = Counter(segged_title)
            total_title_counter += tf_title
        abstract_title_list = [k for (k,v) in list(total_title_counter.most_common(3))]
        abstract_title = ' '.join(abstract_title_list)
        return abstract_title
    
    def get_sampling_abstract_content(self, news_pool):
        k_choice = min(10, len(news_pool)-1)
        sampling = random.choices(list(news_pool.keys()), k=k_choice)
        abstract_content_list = list()
        for news_id in sampling:
            title = news_pool[news_id]['title']
            abstract_content_list.append(title)
        abstract_content = ' '.join(abstract_content_list)
        return abstract_content
    
