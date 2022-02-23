# -*- coding: utf-8 -*-
"""
Created on Wed Sep  9 17:29:35 2020

@author: user
"""

import requests


edit_url = 'http://39.108.250.102:40000/calculate'


def svc(url, input_dict):
    response = requests.post(url=url, json=input_dict)
    return response.json() if response.ok else False


def event_potential_news(event_id, page):
    # 输入 event_id, page 页码， filtered_news 对应页码的相似度过滤后新闻
    input_dict = {
            'model_name': 'event_potential_news_model',
            'model_version': '1.0',
            'event_id':event_id,
            'page': page
            }
    r = svc(edit_url,input_dict)
    return r['filtered_news']


def news_abstract(content_id):
    # 输入 content_id, 输出 first_three_sent 首三句 abstract 摘要 baidu_abstract 百度摘要
    input_dict = {
            'model_name': 'news_abstract_model',
            'model_version': '1.0',
            'content_id':content_id
            }
    r = svc(edit_url,input_dict)
    return r['first_three_sent'], r['abstract'], r['short_baidu_abstract'], r['long_baidu_abstract']


def cluster_abstract(content_id_list,max_sent):
    # 输入 content_id_list content_id的列表，max_sent 最大摘要句子数int， 输出 cluster_abstract 新闻组摘要
    input_dict = {
            'model_name': 'cluster_abstract_model',
            'model_version': '1.0',
            'content_id_list':content_id_list,
            'max_sent':max_sent
            }
    r = svc(edit_url,input_dict)
    return r['cluster_abstract']


def edit_news(content_id, title, short_abstract, abstract, highlight):
    # 输入 content_id 新闻id, title 编辑后新闻标题, short_abstract 编辑后简短摘要, abstract 编辑后摘要, highlight 输出 status 是否成功
    input_dict = {
            'model_name': 'edit_news_model',
            'model_version': '1.0',
            'content_id':content_id,
            'title':title,
            'short_abstract': short_abstract,
            'abstract':abstract,
            'highlight': highlight
            }
    r = svc(edit_url,input_dict)
    return r['status']


def edit_news_module(event_id, content_id, news_module):
    # 输入 content_id 新闻id, news_module 新闻模块（1-7int）, 输出 status 是否成功
    input_dict = {
            'model_name': 'edit_news_module_model',
            'model_version': '1.0',
            'event_id':event_id,
            'content_id':content_id,
            'news_module':news_module
            }
    r = svc(edit_url,input_dict)
    return r['status']    


def potential_event(page):
    # 输入 page 页数 int, 输出 event_list 事件列表
    input_dict = {
            'model_name': 'potential_event_model',
            'model_version': '1.0',
            'page':page
            }
    r = svc(edit_url,input_dict)
    return r['event_list']      


def edit_event(event_id, abstract_title, pic_url, event_type, direction_guide):
    # 输入 事件id 事件名称 图片url 和 事件类型
    input_dict = {
            'model_name': 'event_type_model',
            'model_version': '1.0',
            'event_id': event_id,
            'abstract_title': abstract_title,
            'pic_url': pic_url,
            'event_type': event_type,
            'direction_guide': direction_guide
            }
    r = svc(edit_url,input_dict)
    return r['status'] 


def edit_side_note(event_id, content_id, note_title, note_url, note_text, text_type):
    # 输入事件id和, content id（可以为空str）, 标题, 内容（url）, 以及内容类型（0:文字 1:图片 2:视频）
    input_dict = {
            'model_name': 'edit_side_note_model',
            'model_version': '1.0',
            'event_id': event_id,
            'content_id': content_id,
            'note_title': note_title,
            'note_url': note_url,
            'note_text': note_text,
            'text_type': text_type,
            }
    r = svc(edit_url, input_dict)
    return r['status']


def event_timeline(event_id):
    # 输入事件id int, 输出 event_timeline 时间线新闻列表, 每个元素为一个字典 content_id, publish_time, title
    input_dict = {
            'model_name': 'event_timeline_model',
            'model_version': '1.0',
            'event_id': event_id,
            }
    r = svc(edit_url, input_dict)
    return r['event_timeline']


def event_module(event_id):
    # 输入事件id int, 输出 event_module 事件模块新闻字典, 每个key（模块中文名）对应一个list，
    # list中每个元素为一个字典 content_id, publish_time, title, abstract
    input_dict = {
            'model_name': 'event_module_model',
            'model_version': '1.0',
            'event_id': event_id,
            }
    r = svc(edit_url, input_dict)
    return r['event_module']


def baidu_abstract(content_id, abstract_len):
    # 输入 content_id, abstract_len 摘要长度， 输出 baidu_abstract 百度摘要
    input_dict = {
            'model_name': 'baidu_abstract_model',
            'model_version': '1.0',
            'content_id': content_id,
            'abstract_len': abstract_len
            }
    r = svc(edit_url, input_dict)
    return r['baidu_abstract']


def add_vote_question(event_id, question_name, pos_name, neg_name):
    # 输入 event_id, 对应的投票问题 以及正负观点如能/不能
    input_dict = {
            'model_name': 'edit_vote_question_model',
            'model_version': '1.0',
            'event_id': event_id,
            'question_name': question_name,
            'pos_name': pos_name,
            'neg_name': neg_name
            }
    r = svc(edit_url, input_dict)
    return r['status']


def add_event_recommendation(event_id, recommend_event_id_list):
    # 推荐模块 增加推荐的瓜的id
    input_dict = {
            'model_name': 'edit_recommendation_model',
            'model_version': '1.0',
            'event_id': event_id,
            'recommend_event_id_list': recommend_event_id_list
            }
    r = svc(edit_url, input_dict)
    return r['status']


def edit_timeline(event_id, content_id, mode):
    # 增减时间线操作 add为增 remove为删
    input_dict = {
            'model_name': 'edit_timeline_model',
            'model_version': '1.0',
            'event_id': event_id,
            'content_id': content_id,
            'mode': mode,
            }
    r = svc(edit_url, input_dict)
    return r['status']


def fetch_highlight(text):
    input_dict = {
            'model_name': 'highlight_model',
            'model_version': '1.0',
            'text': text
            }
    r = svc(edit_url, input_dict)
    return r['highlight']


def find_melon_by_key_words(key_words, ts_in_min):
    # key_words -> list of string; ts_in_min: begin time, int like 202010020000
    input_dict = {
            'model_name': 'find_melon_model',
            'model_version': '1.0',
            'key_words': key_words,
            'ts_in_min': ts_in_min
            }
    r = svc(edit_url, input_dict)
    return r['event_list']


def generate_melon_by_key_words(key_words, operator, minimum_should_match, only_title, start_time, end_time, mode):
    # key_words -> list of string
    # operator -> 'and' / 'or'
    # minimum_should_match -> only available in 'or' operation
    # only_title -> bool True or False
    # start_time/end_time -> int like 202009200000
    # mode: generally empty string, if 'evolve', then mimic pipeline from end time
    # if event_id is -1: not with enough news, -2: no news at all
    input_dict = {
            'model_name': 'generate_melon_model',
            'model_version': '1.0',
            'key_words': key_words,
            'operator': operator,
            'page_size': 10000,
            'minimum_should_match': minimum_should_match,
            'only_title': only_title,
            'source': 'news',
            'start_time': start_time,
            'end_time': end_time,
            'mode': mode
            }
    r = svc(edit_url, input_dict)
    return r['event_id']


def ES_search(keywords, operator, minimum_should_match, only_title, start_time, end_time):
    input_dict = {
            'keywords': keywords,
            'operator': operator,
            'page_size': 10000,
            'minimum_should_match': minimum_should_match,
            'only_title': only_title,
            'source': 'news',
            'start_time': start_time,
            'end_time': end_time
    }
    url = 'http://172.18.223.14:8903/search_news_ids'
    response = requests.post(url=url, json=input_dict)
    ES_search_id_list = response.json()['ids']
    return ES_search_id_list


def generate_timeline(event_id, event_type):
    input_dict = {
            'model_name': 'generate_timeline_model',
            'model_version': '1.0',
            'event_id': event_id,
            'event_type': event_type
            }
    r = svc(edit_url, input_dict)
    return r['status']

def compare_timeline(event_id, cut_time):
    # 比较 竞技场时间线与时间片聚类时间线效果并提供新闻参数集
    input_dict = {
            'model_name': 'compare_timeline_model',
            'model_version': '1.0',
            'event_id':event_id,
            'cut_time':cut_time
            }
    r = svc(edit_url,input_dict)
    return r['arena_timeline'], r['agglo_timeline'], r['news_pool']

def awaken_melon(event_id, start_time):
    input_dict = {
            'model_name': 'awaken_melon_model',
            'model_version': '1.0',
            'event_id': event_id,
            'start_time': start_time
            }
    r = svc(edit_url, input_dict)
    return r['status']


if __name__ == "__main__":
    # res = edit_side_note(393, 'EVENT',
    #                      '课代表在此',
    #                      '北京近日下达了一系列新政策，包括建设人工智能创新实验基地、交通新基建、数字金融机构、'
    #                      '自由贸易区等等，北京将迎来新一轮蓬勃发展。',
    #                      0)
    # print(res)
    # t = baidu_abstract("b7c74a2c408d6ce5", 50)
    # print(t)
    # r = edit_event(614, '加州大火', 'https://image.stheadline.com/f/1500p0/0x0/100/none/430ab1d7f10b57f3aa3a17aeef65ba13/stheadline/news_res/2020/09/08/672381/i_src_605513892.jpg', 0)
    # print(r)
    # r = add_event_recommendation(600, [601,602,603])
    # print(r)
    # r = find_melon_by_key_words(['特朗普'], 202009010000)
    # r = generate_melon_by_key_words(['苹果'], str(), '2020-10-12', '2020-10-16')
    # print(r)
    # r = generate_melon_by_key_words(['美', '大选'], 'and', 2, True, 202010010000, 202010030000, 'evolve')
    # print(r)
    # ES_search(['板蓝根'], 'and', 1, True, '2020-10-15', '2020-10-19')
    # r = edit_event(145, str(), 'https://ptf.flyert.com/forum/201809/07/211406c3l2g903zytetkfk.jpg!k', 1, '空铁联运')
    # print(r)
    print(awaken_melon(320, 202010290000))

