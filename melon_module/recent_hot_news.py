# -*- coding: utf-8 -*-

from melon_module.abstract import abstract

class recent_hot_news:
    def __init__(self):
        self.abstract = abstract()
        
    def get_recent_hot_news(self, news_cluster, top_num):
        recent_hot_news = list()
        # sorted_pool = sorted(news_cluster_pool.items(), key=lambda item: len(item[1]['news_collection']), reverse=True)
        sorted_cluster = sorted(news_cluster.items(), key=lambda item: item[1]['repost_num'], reverse=True)
        for _, cluster_info in sorted_cluster:
            cluster_news_info = cluster_info['cluster_news_info']
            if len(cluster_news_info) < 100:
                abstract_title = self.abstract.get_word_abstract_title(cluster_news_info)
                content_id = (sorted(cluster_news_info, key=lambda x:cluster_news_info[x]['repost_num'], reverse=True))[0]
                recent_hot_news.append({
                    'content_id': content_id,
                    'hotness': cluster_info['repost_num'],
                    'abstract_title': abstract_title,
                })

            if len(recent_hot_news) >= top_num:
                break
        return recent_hot_news

