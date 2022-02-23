# -*- coding: utf-8 -*-

# This file provides functions for creating leaf

import numpy as np
from datetime import datetime,timedelta
from collections import Counter,defaultdict
from sklearn.cluster import AgglomerativeClustering
from gensim import similarities

class simi_filter:
    def __init__(self):
        a = None

    def filter_simi_news(self, news_collection, news_pool):
        news_w2v = list()
        limit_publish_time = news_pool[news_collection[-1]]['publish_time'] - timedelta(days=1)
        for news_id in news_collection:
            corpus = news_pool[news_id]['corpus']
            news_w2v.append(corpus)
        index = similarities.MatrixSimilarity(news_w2v,num_features=300)
        potential_leaf = list()
        latest_news_list = list()
        for news_id in news_collection:
            publish_time = news_pool[news_id]['publish_time']
            corpus = news_pool[news_id]['corpus']
            if publish_time >= limit_publish_time:
                simi_newsid = self.get_latest_similar_news(news_collection,index,news_id,corpus,0.95)
                if not simi_newsid:
                    latest_news_list.append(news_id)
            else:
                simi_newsid = self.get_similar_news(news_collection,index,news_id,corpus,0.95)            
                if not simi_newsid:
                    potential_leaf.append(news_id)
        
        return potential_leaf,latest_news_list