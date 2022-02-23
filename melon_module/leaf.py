# -*- coding: utf-8 -*-

# This file provides functions for creating leaf

import numpy as np
from datetime import datetime,timedelta
from collections import Counter,defaultdict
#from melon_modules_json.database import Database
from sklearn.cluster import AgglomerativeClustering
from gensim import similarities

class Leaf:
    def __init__(self):
#        self.database = Database()
        a = 0
    
    def find_begining(self, news_to_sort_list, news_pool):
        limit_pub = news_pool[news_to_sort_list[0][0]]['publish_time'] + timedelta(hours=6)
        news_count = defaultdict(int)
        for news_id,timestamp in news_to_sort_list:
            if news_pool[news_id]['publish_time'] >= limit_pub:
                break
            news_count[news_id]+=1
        
        highest_count = 0
        begining = None
        for news_id,count in news_count.items():
            if count>highest_count:
                begining = news_id
        return begining        
        
    def make_input_X(self, potential_leaf, news_pool):
        X_list = list()
        news_id_list = list()
        news_to_sort_list = list()
        for news_id in potential_leaf:
            # publish_time = news_pool[news_id]['publish_time']
            # timestamp = publish_time.timestamp()
            timestamp = news_pool[news_id]['timestamp']
            news_to_sort_list.append([news_id,timestamp])
        news_to_sort_list = sorted(news_to_sort_list, key=lambda k: k[1])
        begining = self.find_begining(news_to_sort_list,news_pool)
        for news_id, timestamp in news_to_sort_list:
            if news_id != begining:
                X_list.append([0, timestamp])
                news_id_list.append(news_id)            
        X = np.array(X_list)
        return X, news_id_list,begining

    def leaf_cluster(self, X, cluster_num=15):
        clustering = AgglomerativeClustering(n_clusters=cluster_num).fit(X)
        return clustering.labels_

    def get_clip_dict(self, labels, news_id_list):
        clip_dict = dict()
        if len(labels) == len(news_id_list):
            for i in range(0, len(labels)):
                clip_result_list = clip_dict.get(str(labels[i]), list())
                clip_result_list.append(news_id_list[i])
                clip_dict[str(labels[i])] = clip_result_list
        return clip_dict
    
    def get_TF_per_clip(self, clip_dict, news_pool):
        TF_all_clip_dict = dict()
        for clip_id, clip in clip_dict.items():
            TF_counter = Counter()
            for news_id in clip:
                TF_per_news = news_pool[news_id]['tf_per_news']
                TF_counter += TF_per_news
            TF_all_clip_dict[clip_id] = TF_counter
        return TF_all_clip_dict
    
    def find_best_news(self, clip_dict, TF_all_clip_dict, news_pool, begining):
        best_news_list = [begining]
#        news_w2v.append(news_pool[begining]['corpus'])
#        index = similarities.MatrixSimilarity(news_w2v,num_features=300)
        for key in clip_dict:
            TF_for_clip_i = TF_all_clip_dict[key]
            clip_i = clip_dict[key]
            record = 0
            best_news = None
            clip_id_count = defaultdict(int)
            for news_id in clip_i:
                score = 0
                clip_id_count[news_id]+=1
                top_entity_per_news = news_pool[news_id]['top_word_per_news']
                for entity in top_entity_per_news:
                    if entity not in TF_for_clip_i.most_common(1):
                        score += TF_for_clip_i.get(entity, 0)
#                if score > record and self.get_similar_news(best_news_list,index,news_id,news_pool[news_id]['corpus'],0.9):
                if score > record:
                    record = score
                    best_news = news_id
            if best_news:
                best_news_list.append(best_news)
#                news_w2v.append(news_pool[best_news]['corpus'])
#                index = similarities.MatrixSimilarity(news_w2v,num_features=300)
        return best_news_list
    
    def sort_news_by_publish_time(self, best_news_list, news_pool):
        news_to_sort_dict = dict()
        for news_id in best_news_list:
            publish_time = news_pool[news_id]['publish_time']
            timestamp = publish_time.timestamp()
            news_to_sort_dict[news_id] = int(timestamp)
        news_to_sort_dict = {k: v for k, v in sorted(news_to_sort_dict.items(), key=lambda item:item[1])}
        return list(news_to_sort_dict.keys())

    def make_leaf_for_news_collection(self, news_collection, news_pool, max_num = 10):
        potential_leaf,latest_news_list = self.filter_simi_news(news_collection, news_pool)
        if len(set(potential_leaf))>=max_num:
            X, news_id_list, begining = self.make_input_X(potential_leaf, news_pool)
            cluster_num = max_num
            labels = self.leaf_cluster(X, cluster_num)
            clip_dict = self.get_clip_dict(labels, news_id_list)
            TF_all_clip_dict = self.get_TF_per_clip(clip_dict, news_pool)
            best_news_list = self.find_best_news(clip_dict, TF_all_clip_dict, news_pool,begining)
            best_news_list_sorted = self.sort_news_by_publish_time(best_news_list, news_pool)
            best_news_list_sorted += latest_news_list
        else:
            news_set = set()
            best_news_list_sorted = list()
            for news_id in potential_leaf:
                if news_id not in news_set:
                    best_news_list_sorted.append(news_id)
                    news_set.add(news_id)
            best_news_list_sorted += latest_news_list

        return best_news_list_sorted
    
    def get_similar_news(self,news_list,index,content_id, corpus, threshold):
        if index:
            similar_news = zip(news_list,index[corpus])
            similar_news_limited = [x for x in similar_news if x[1]>=threshold]
            if len(similar_news_limited) > 0:
                if similar_news_limited[0][0] == content_id:
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

    def filter_simi_news(self, news_collection, news_pool):
        simi_dict = dict()
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
            repost_num = news_pool[news_id]['repost_num']
            if publish_time >= limit_publish_time:
                simi_newsid = self.get_latest_similar_news(news_collection,index,news_id,corpus,0.95)
                if not simi_newsid:
                    latest_news_list.append(news_id)
            else:
                simi_newsid = self.get_similar_news(news_collection,index,news_id,corpus,0.95)            
                if simi_newsid:
                    if not simi_dict.get(simi_newsid):
                        simi_dict[news_id] = simi_newsid
                    else:
                        simi_newsid = simi_dict[simi_newsid]
                        simi_dict[news_id] = simi_newsid
                    for i in range(0,repost_num):
                        potential_leaf.append(simi_newsid)
                else:
                    for i in range(0,repost_num):
                        potential_leaf.append(news_id)
        
        return potential_leaf,latest_news_list
