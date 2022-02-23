# -*- coding: utf-8 -*-

from datetime import datetime,timedelta
from collections import Counter,defaultdict
from melon_module.news_simi_module import news_simi_module
from melon_module.display_filter_module import display_filter_module

class story_timeline:
    def __init__(self):
        self.news_simi_mod = news_simi_module()
        self.display_filter_mod = display_filter_module()

    def story_timeline_init(self, news_collection, news_pool, event_info, timeline_len):
        valid_news = self.display_filter_mod.check_valid_display_news(news_collection, news_pool, event_info)
        if len(valid_news) <= timeline_len:
            return valid_news
        timeline_valid_news = self.news_simi_mod.duplicate_filter(valid_news, valid_news, news_pool)
        story_timeline = self.history_arena(timeline_valid_news, news_collection, news_pool, event_info, timeline_len)
        return story_timeline

    def story_timeline_update(self, human_dict, history_list, timeline_potential_news, news_collection, news_pool, event_info, timeline_len):
        valid_updated_news = self.display_filter_mod.check_valid_display_news(timeline_potential_news, news_pool, event_info)
        if len(valid_updated_news)==0 and len(history_list) <= timeline_len:
            return history_list
        updated_history_list = history_list + valid_updated_news    
        timeline_valid_news = self.news_simi_mod.duplicate_filter(valid_updated_news, updated_history_list, news_pool)
        timeline_valid_news = history_list + timeline_valid_news
        if len(timeline_valid_news)<=timeline_len:
            return timeline_valid_news 
        story_timeline = self.history_arena(timeline_valid_news, news_collection, news_pool, event_info, timeline_len)
                
        return story_timeline

    def history_arena(self, history_competitor, news_collection, news_pool, event_info, history_len):
        if len(history_competitor)<=history_len:
            return history_competitor
        index = self.news_simi_mod.get_news_index(news_collection, news_pool)    
        simi_dict = dict()
        simi_count = defaultdict(int)
        for news_id in news_collection:
            corpus = news_pool[news_id]['corpus']
            repost_num = news_pool[news_id]['repost_num']
            simi_newsid = self.news_simi_mod.get_similar_news(news_collection,index,news_id,corpus,0.95)
            if simi_newsid:
                if not simi_dict.get(simi_newsid):
                    simi_dict[news_id] = simi_newsid
                    simi_count[simi_newsid] += repost_num
                else:
                    simi_newsid = simi_dict[simi_newsid]
                    simi_dict[news_id] = simi_newsid
                    simi_count[simi_newsid] += repost_num
        
        prev_time = news_pool[history_competitor[0]]['publish_time']
        end_time = news_pool[history_competitor[-1]]['publish_time']
        period = end_time - prev_time
        event_top_word = event_info['event_top_word']
        score_dict = dict()
        for news_id in history_competitor[1:]:
            repost_num = news_pool[news_id]['repost_num']
            simi_score = simi_count[news_id]
            publish_time = news_pool[news_id]['publish_time']
            source_id = news_pool[news_id]['source_id']
            title = news_pool[news_id]['title']
            media_score = 0
            if self.display_filter_mod.source_info.get(source_id):
                if self.display_filter_mod.source_info[source_id]['rank'] == 1:
                    media_score = 5
                elif self.display_filter_mod.source_info[source_id]['rank'] == 0:
                    media_score = 10
            
            topic_score = 0
            for top_word in event_top_word[:5]:
                if top_word in title:
                    topic_score += 2
    
            time_gap = publish_time - prev_time
            # time_ratio = 1/ (math.log(period / (time_gap + timedelta(hours=1))) + 1)
            time_ratio = 1/ ((period / (time_gap + timedelta(hours=1)))+1)
            # print(time_ratio)
            score = (repost_num*2 + simi_score + topic_score + media_score) * time_ratio
            score_dict[news_id] = score
            prev_time = publish_time
        
        sorted_list = sorted(score_dict.items(), key=lambda x:x[1], reverse=True)
        history_winner_set = set()
        for news_id in sorted_list:
            if len(history_winner_set) < history_len-1:
                history_winner_set.add(news_id[0])
        history_winner_set.add(history_competitor[0])
        history_winner = list()
        for news_id in history_competitor:
            if news_id in history_winner_set:
                history_winner.append(news_id)
        return history_winner

