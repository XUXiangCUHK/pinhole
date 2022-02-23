# -*- coding: utf-8 -*-

from melon_module.story_timeline import story_timeline

class event_timeline:
    def __init__(self):
        self.story_tl = story_timeline()
    
    def update_timeline(self, event_info, history_timeline, timeline_potential_news, human_dict, news_collection, news_pool):
        event_type = event_info.get('event_type',0)
        if event_type == 0:
            updated_timeline = self.story_tl.story_timeline_update(human_dict, history_timeline, timeline_potential_news, news_collection, news_pool, event_info, 7)
        else:
            updated_timeline = self.story_tl.story_timeline_update(human_dict, history_timeline, timeline_potential_news, news_collection, news_pool, event_info, 4)
        return updated_timeline
        
