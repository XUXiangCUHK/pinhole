# -*- coding: utf-8 -*-
import requests
from models.database import Database
from collections import Counter, defaultdict
import time
from urllib.parse import urlencode
from urllib.request import urlopen
from urllib.error import URLError
from urllib.request import Request
import json
from textrank4zh import TextRank4Keyword, TextRank4Sentence

class Abstract:
    def __init__(self):
        self.database = Database()
        self.OPINION_SERVICE_URL = 'http://120.79.228.104:16006/get_news_opinions'
        self.news_summary_url = 'http://47.106.46.242:30009/calculate'
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

    def get_news_abstract(self, title, content, max_summary_len=50):
        data = {'title': title, 'content': content[:2900], 'max_summary_len': max_summary_len}
        result = requests.post(self.summary_url, data=json.dumps(data)).json()
        # 防止qps超限
        time.sleep(0.5)
        return result.get('summary')

    def get_highlight_sent(self, text):
            self.tr4s.analyze(text=text, lower=True, source = 'all_filters')

            for item in self.tr4s.get_key_sentences(num=2):
                if item.index == 0:
                    continue
                else:
                    return item.sentence

    def make_abstract_for_key_news_list(self, key_news_list, news_pool):
        abstract_info = list()
        print(key_news_list)
        for news_id in key_news_list:
            title = news_pool[news_id]['title']
            content = news_pool[news_id]['content']
            short_abstract = self.get_news_abstract(title, content)
            abstract = self.get_news_abstract(title, content, 300)
            if abstract:
                highlight = self.get_highlight_sent(abstract)
            else:
                highlight = str()
            abstract_info.append({
                'content_id': news_id,
                'title': title,
                'short_abstract': short_abstract,
                'abstract': abstract,
                'highlight': highlight
            })
        self.database.insert_news_abstract(abstract_info)
    
    # make event info
    def get_abstract_title(self, news_collection, news_pool):
        total_title_counter = Counter()
        for news_id in news_collection:
            segged_title = news_pool[news_id]['segged_title']
            tf_title = Counter(segged_title)
            total_title_counter += tf_title
        abstract_title_list = [k for (k,v) in list(total_title_counter.most_common(3))]
        abstract_title = ' '.join(abstract_title_list)
        return abstract_title
    
    def get_abstract_content(self, key_news_list, news_pool):
        abstract_content_list = list()
        for news_id in key_news_list:
            title = news_pool[news_id]['title']
            abstract_content_list.append(title)
        abstract_content = ' '.join(abstract_content_list)
        return abstract_content
    
    def get_first_three_sentence(self, content_id=None, content=None):
        if not content:
            content = self.database.get_news_content(content_id)
        content_split = content.split('。')
        length = min(3, len(content_split))
        return '。'.join(content_split[0:length])
    
    # Yulong API
    def abstract_API(self, news_ids_str):
        try:
            response = requests.post(self.OPINION_SERVICE_URL, json={'news_ids': news_ids_str})
            response.raise_for_status()
            return response.json()['result']
        except:
            print('Get cluster summary of {} fail'.format(news_ids_str))
            return None

    def get_formal_abstract_content(self, news_ids_list):
        list_of_news_ids_list = list()
        list_of_news_ids_list.append(news_ids_list)
        news_ids_str = str(list_of_news_ids_list)
        return self.abstract_API(news_ids_str)
    
    def get_formal_abstract_for_key_news_list(self, key_news_list):
        list_of_news_ids_list = list()
        for news_id in key_news_list:
            list_of_news_ids_list.append([news_id])
        news_ids_str = str(list_of_news_ids_list)
        abstract_list = self.abstract_API(news_ids_str)

        abstract_info = list()
        for news_id, abstract in zip(key_news_list, abstract_list):
            abstract_info.append({
                'content_id': news_id,
                'abstract': abstract,
            })
        return abstract_info
        # self.database.insert_news_abstract(abstract_info)
    
    # first paragraph
    def get_first_paragraph_for_key_news_list(self, key_news_list):
        abstract_info = list()
        for news_id in key_news_list:
            abstract = self.get_first_three_sentence(news_id)
            abstract_info.append({
                'content_id': news_id,
                'abstract': abstract,
            })
        return abstract_info
    
    # Neal api
    def neal_summary_api(self, content):
        news_summary_svc_input = {
                'model_name': 'news_summary',
                'model_version': '1.0',
                'raw_text': content,
                "limit":3,
                "process_raw":True
            }
        try:
            response = requests.post(self.news_summary_url, json=news_summary_svc_input)
            return response.json()['summary']
        except:
            print('Get cluster summary of {} fail'.format('Neal'))
            return None
    
    def get_summary(self, key_news_list):
        abstract_info = list()
        for news_id in key_news_list:
            content = self.database.get_news_content(news_id)
            abstract = self.neal_summary_api(content)
            abstract_info.append({
                'content_id': news_id,
                'abstract': abstract,
            })
        return abstract_info