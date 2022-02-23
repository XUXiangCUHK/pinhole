# -*- coding: utf-8 -*-

# This file provides functions for tagging

from datetime import datetime
from collections import defaultdict
from melon_modules.news_simi_module import news_simi_module
from melon_modules.display_filter_module import display_filter_module


class latest_module:
    def __init__(self):
        self.latest_module_type = 8
        self.news_simi_mod = news_simi_module()
        self.display_filter_mod = display_filter_module()
    
    def form_latest_module(self, event_updated_news, event_info, news_pool):
        event_latest_news = list()
        delete_info = list()
        for event_id, updated_news in event_updated_news.items():
            event_dict = event_info[event_id]
            event_type = event_dict.get('event_type', -1)
            if event_type>=0:
                valid_latest_news = self.display_filter_mod.check_valid_display_news(updated_news, news_pool, event_dict)
                if valid_latest_news!=list():
                    previous_latest_news = event_info[event_id].get('latest_news',list())
                    simi_check_news = previous_latest_news + valid_latest_news
                    valid_latest_news = self.news_simi_mod.duplicate_filter(valid_latest_news, simi_check_news, news_pool)
                if valid_latest_news!=list():
                    delete_info.append((event_id, self.latest_module_type))
                    latest_news = valid_latest_news + previous_latest_news
                    for content_id in latest_news[:3]:
                        event_latest_news.append({
                            'event_id': event_id,
                            'content_id': content_id,
                            'news_module': self.latest_module_type
                        })
        return event_latest_news, delete_info
    
    def event_generate_latest_news(self, event_id, event_related_news, news_pool):
        event_latest_news = list()
        delete_info = list()
        valid_latest_news = self.display_filter_mod.check_valid_display_news(event_related_news, news_pool, dict())
        if valid_latest_news!=list():
            valid_latest_news = self.news_simi_mod.duplicate_filter(valid_latest_news, valid_latest_news, news_pool)
        if valid_latest_news!=list():
            delete_info.append((event_id, self.latest_module_type))
            latest_news = valid_latest_news
            for content_id in latest_news[:3]:
                event_latest_news.append({
                    'event_id': event_id,
                    'content_id': content_id,
                    'news_module': self.latest_module_type
                })
        return event_latest_news, delete_info