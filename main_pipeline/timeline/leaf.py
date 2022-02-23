# -*- coding: utf-8 -*-

# This file provides functions for creating leaf

import numpy as np
from datetime import datetime
from collections import Counter
from sklearn.cluster import AgglomerativeClustering

class Leaf:
    def __init__(self):
        a = 0
        
    def make_input_X(self, news_collection, news_pool):
        X_list = list()
        news_id_list = list()
        news_to_sort_dict = dict()
        for news_id in news_collection:
            publish_time = news_pool[news_id]['publish_time']
            timestamp = publish_time.timestamp()
            news_to_sort_dict[news_id] = int(timestamp)
        news_to_sort_dict = {k: v for k, v in sorted(news_to_sort_dict.items(), key=lambda item:item[1])}
        for news_id, timestamp in news_to_sort_dict.items():
            X_list.append([0, timestamp])
            news_id_list.append(news_id)
        X = np.array(X_list)
        return X, news_id_list

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
    
    def find_best_news(self, clip_dict, TF_all_clip_dict, news_pool):
        best_news_list = list()
        for i in range(0, len(TF_all_clip_dict)):
            TF_for_clip_i = TF_all_clip_dict[str(i)]
            clip_i = clip_dict[str(i)]
            record = 0
            best_news = clip_i[0]
            for news_id in clip_i:
                score = 0
                top_entity_per_news = news_pool[news_id]['top_word_per_news']
                for entity in top_entity_per_news:
                    if entity not in TF_for_clip_i.most_common(1):
                        score += TF_for_clip_i.get(entity, 0)
                if score >= record:
                    record = score
                    best_news = news_id
            best_news_list.append(best_news)
        return best_news_list
    
    def sort_news_by_publish_time(self, best_news_list, news_pool):
        news_to_sort_dict = dict()
        for news_id in best_news_list:
            publish_time = news_pool[news_id]['publish_time']
            timestamp = publish_time.timestamp()
            news_to_sort_dict[news_id] = int(timestamp)
        news_to_sort_dict = {k: v for k, v in sorted(news_to_sort_dict.items(), key=lambda item:item[1], reverse=True)}
        return list(news_to_sort_dict.keys())
    
    def make_leaf_for_news_collection(self, news_collection, news_pool, cluster_num=None, max_num=15):
        X, news_id_list = self.make_input_X(news_collection, news_pool)
        if not cluster_num:
            cluster_num = max(int(len(news_collection)/10), 5)
            cluster_num = min(cluster_num, max_num)
        labels = self.leaf_cluster(X, cluster_num)
        clip_dict = self.get_clip_dict(labels, news_id_list)
        TF_all_clip_dict = self.get_TF_per_clip(clip_dict, news_pool)
        best_news_list = self.find_best_news(clip_dict, TF_all_clip_dict, news_pool)
        best_news_list_sorted = self.sort_news_by_publish_time(best_news_list, news_pool)
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