# -*- coding: utf-8 -*-
"""
Created on Fri Nov  6 16:12:54 2020

@author: user
"""
from gensim import similarities

class news_simi_module():
    def __init__(self):
        a = 0
    
    def get_news_index(self, news_id_list, news_pool):
        news_w2v = list()
        for news_id in news_id_list:
            corpus = news_pool[news_id]['corpus']
            news_w2v.append(corpus)
        index = similarities.MatrixSimilarity(news_w2v,num_features=300)
        return index
    
    def get_similar_news(self,news_list,index,news_id, corpus, threshold):
        if index:
            similar_news = zip(news_list,index[corpus])
            similar_news_limited = [x for x in similar_news if x[1]>=threshold]
            if len(similar_news_limited) > 0:
                if similar_news_limited[0][0] == news_id:
                    return None
                else:
                    return similar_news_limited[0][0]
            else:
                return None
        else:
            return None

    def get_latest_similar_news(self,news_list,index,content_id, corpus, threshold):
        if index:
            similar_news = zip(news_list,index[corpus])
            similar_news_limited = [x for x in similar_news if x[1]>=threshold]
            if len(similar_news_limited) > 0:
                if similar_news_limited[-1][0] == content_id:
                    return None
                else:
                    return similar_news_limited[-1][0]
            else:
                return None
        else:
            return None

    def duplicate_filter(self, check_news, simi_news_list, news_pool, threshold = 0.9):
        index = self.get_news_index(simi_news_list, news_pool)
        valid_news = list()
        for news_id in check_news:
            corpus = news_pool[news_id]['corpus']
            simi_newsid = self.get_similar_news(simi_news_list, index, news_id, corpus, threshold)   
            if not simi_newsid:
                valid_news.append(news_id)        
        return valid_news        
    
    