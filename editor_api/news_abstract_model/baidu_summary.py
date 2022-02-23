# -*- coding: utf-8 -*-
"""
Created on Fri Sep 25 15:03:15 2020

@author: user
"""

# coding=utf-8
import json
import time
from pprint import pprint
from urllib.request import urlopen
from urllib.request import Request
from urllib.error import URLError
from urllib.parse import urlencode
import requests
SUMMARY_URL = "https://aip.baidubce.com/rpc/2.0/nlp/v1/news_summary"
class Baidu_NLP():
    def __init__(self, API_KEY, SECRET_KEY):
        token = fetch_token(API_KEY, SECRET_KEY)
        self.summary_url = SUMMARY_URL + "?charset=UTF-8&access_token=" + token
    def make_summary_request(self, title, content, max_summary_len=200):
        data = {'title': title,'content': content,'max_summary_len': max_summary_len}
#        print('\nInput:')
#        pprint(data)
#        print('\nOutput:')
#        pprint(requests.post(self.summary_url, data=json.dumps(data)).json())
        result = requests.post(self.summary_url, data=json.dumps(data)).json()
        # 防止qps超限
        time.sleep(0.5)
        return result['summary']
        
def fetch_token(API_KEY, SECRET_KEY):
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
if __name__ == '__main__':
    API_KEY = 'Po5IwLHcVdvalD3mtb0F8cKG'
    SECRET_KEY = 'zkN29vY0E4vW6ZIXEiNxYDcdD6z8Gslh'
    baidu_nlp_cls = Baidu_NLP(API_KEY, SECRET_KEY)
    # publish_id = 'ec51c000a1c7151a'
    title = '''它来了！它来了！带着百亿净利润来了！这个超级巨无霸 或成近年最大IPO！'''
    content = '''有着23天"闪电"上会经历的京沪高铁上周末拿到了IPO批文，这意味着公司上市在即，或将成为近年最大IPO。
巨无霸京沪高铁上市在即，公司是京沪高速铁路及沿线车站的投资、建设、运营主体。
财务数据显示，京沪高铁2018年的营业收入超过300亿元，净利润超过100亿元。
京沪高铁本次拟发行不超过75.57亿股，发行后总股本不超过503.77亿股；募集资金拟全部用于收购京福安徽公司65.0759%股权，收购对价为500亿元。分析预期，京沪高铁的最高募资规模有望突破350亿元，有望成为近年最大IPO。
值得一提的是，京沪高铁23天的"闪电"上会经历曾刷新了IPO速度，公司于10月22日报送IPO申报稿，之后在11月14日上会获得通过。
（文章来源：央视财经）
(责任编辑：DF380)'''
    max_summary_len = 150 # 200 if absent
    baidu_nlp_cls.make_summary_request(title, content, max_summary_len)