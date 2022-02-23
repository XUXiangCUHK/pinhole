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
        result = requests.post(self.summary_url, data=json.dumps(data)).json()
        # 防止qps超限
        time.sleep(0.5)
        if not result.get('summary'):
            print(result)
        return result.get('summary','failed')

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
    title = '''调查显示：美大选投票率或创百年来新高'''
    content = '''参考消息网10月8日报道据路透社华盛顿10月6日报道，美国大选的提前投票数据显示，在11月前投票的美国人数量创下新高，这暗示特朗普总统与民主党竞选对手拜登之间的这场对决，选民投票率可能创下纪录高位。目前距离11月3日的大选还有四周时间，根据“美国选举计划”统计的提前投票数据，超过380万美国人已经投票，远超过2016年同期的约7.5万人。初期投票人数的激增促使麦克唐纳预计，本次大选的总投票人数将约为1.5亿人，占合格选民总数的65%，为1908年以来的最高值。6日，美国俄亥俄州辛辛那提的选民在排队等待提前投票。'''
    max_summary_len = 100 # 200 if absent
    result = baidu_nlp_cls.make_summary_request(title, content, max_summary_len)
    print(result)