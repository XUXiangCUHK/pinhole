from datetime import datetime, timedelta

from database_related.event_data_manipulator import event_data_manipulator
from database_related.news_data_manipulator import news_data_manipulator
from timeline.event_timeline import event_timeline

from melon_modules.abstract import abstract
from melon_modules.tag import tag
from melon_modules.recent_hot_news import recent_hot_news
from melon_modules.event_manipulator import event_manipulator
from melon_modules.latest_news_module import latest_module

class main_panel():
    def __init__(self):
        self.event_dm = event_data_manipulator()
        self.news_dm = news_data_manipulator()
        self.et = event_timeline()
        self.abt = abstract()
        self.tag = tag()
        self.rhn = recent_hot_news()
        latest_event_id = self.event_dm.get_initial_event_id()
        self.event_manipulator = event_manipulator(latest_event_id)
        self.latest_md = latest_module()
     
    def event_update_timeline(self, event_updated_news, event_info, news_pool):
        event_timeline_insert = list()
        for event_id,updated_news in event_updated_news.items():
            event_type = event_info[event_id].get('event_type', -1)
            if event_type>=0:
                print(event_id, event_type)
                history_timeline = event_info[event_id].get('history_timeline')
                human_dict = dict()
                news_collection = event_info[event_id].get('news_id_list')
                updated_timeline = self.et.update_timeline(event_info[event_id], history_timeline, updated_news, human_dict, news_collection, news_pool)
                event_info[event_id]['updated_timeline'] = updated_timeline
                for content_id in updated_timeline:
                    insert_dict = dict()
                    insert_dict['event_id'] = event_id
                    insert_dict['content_id'] = content_id
                    event_timeline_insert.append(insert_dict)
        return event_info, event_timeline_insert

    def new_event_info_init(self, new_event_cluster, news_cluster):
        all_new_event_related_news = list()
        all_new_entity_related_event = list()
        all_new_event_info = list()
        for event_id, cluster_id in new_event_cluster:
            cluster_info = news_cluster[cluster_id]
            content_id_list = list(cluster_info['cluster_news_info'].keys())
            event_news_pool = self.news_dm.get_news_info_from_list(content_id_list)
            event_related_news, entity_related_event, event_info = self.event_manipulator.new_event_info_init(event_id, cluster_info, event_news_pool)
            all_new_event_related_news += event_related_news
            all_new_entity_related_event += entity_related_event
            all_new_event_info += event_info
        return all_new_event_related_news, all_new_entity_related_event, all_new_event_info

    def update_event_info(self, updated_event_id, all_event_info):
        updated_entity_related_event = list()
        updated_event_info = list()
        for event_id in updated_event_id:
            news_id_list = all_event_info[event_id]['news_id_list']
            event_news_pool = self.news_dm.get_news_info_from_list(news_id_list)
            entity_related_event, event_info = self.event_manipulator.update_event(event_id, event_news_pool)
            updated_entity_related_event += entity_related_event
            updated_event_info += event_info
        return updated_entity_related_event, updated_event_info

    def get_calculate_period(self, end_time, gap=30):
        start_time = end_time - timedelta(minutes=gap)
        start_time_str = start_time.strftime("%Y-%m-%d %H:%M")
        end_time_str = end_time.strftime("%Y-%m-%d %H:%M")
        return start_time_str, end_time_str
    
    
    def pipeline_main(self):
        # confirm time
#         time_now = datetime.now()
#         ts_in_min = time_now.strftime('%Y%m%d%H%M')
# #        ts_in_min = '190001010000'
#         start_time_str, end_time_str = self.get_calculate_period(time_now, gap = 30)
        
#         # create news_pool waiting for tag
#         tag_news_pool = self.news_dm.get_tag_news_info(start_time_str, end_time_str)
        
#         # create existing event_pool
#         ts_before = (time_now - timedelta(days = 15)).strftime('%Y%m%d%H%M')
#         event_info = self.event_dm.get_all_event_info(ts_before)
        
        # tag news
        # updated_event_id, event_updated_news, updated_event_related_news = self.tag.tag_input_news(tag_news_pool, event_info)
        # event_related_news = [{'event_id': 671, 'content_id': '4ed8f3c6a4b40673'}, {'event_id': 671, 'content_id': '95c54e9e0971dec8'},{'event_id': 818, 'content_id': '4ed8f3c6a4b40673'}, {'event_id': 818, 'content_id': '95c54e9e0971dec8'}]
        event_id_list = [127, 802, 823, 600, 736, 818]
        event_latest_news, delete_info = self.latest_md.form_latest_module(event_id_list)
        self.news_dm.delete_from_news_module(delete_info)
        self.news_dm.insert_latest_news_into_news_module(event_latest_news)

        # delete_info = self.latest_md.form_delete_info(event_related_news)

        # updated_entity_related_event, updated_event_info = self.update_event_info(updated_event_id, event_info)
        # self.news_dm.insert_event_related_news(updated_event_related_news, ts_in_min)
        # self.event_dm.insert_entity_related_event(updated_entity_related_event, ts_in_min)
        # self.event_dm.insert_event_info(updated_event_info, ts_in_min)
        # self.event_dm.insert_event_latest_info(updated_event_info, ts_in_min)        
        
        # # auto-made timeline according to event type
        # important_event_news_pool = self.news_dm.get_important_event_news_pool(event_info, updated_event_id)
        # timeline_updated_event_info, event_timeline_insert = self.event_update_timeline(event_updated_news, event_info, important_event_news_pool)
        # news_abstract_list = self.abt.get_updated_important_event_abstract(timeline_updated_event_info, important_event_news_pool)
        
        # self.event_dm.insert_event_timeline(event_timeline_insert, ts_in_min)
        # self.news_dm.insert_news_abstract(news_abstract_list)
        # # 24h interval for cluster
        # start_time_24h, end_time_24h = self.get_calculate_period(time_now, gap=60*24)
        # publish_time_48h, _ = self.get_calculate_period(time_now, gap=60*24*2)
        # # cluster
        # news_cluster = self.news_dm.get_news_cluster(start_time_24h, end_time_24h, publish_time_48h)
        # # get 24h hot news
        # recent_hot_news = self.rhn.get_recent_hot_news(news_cluster, top_num=20)
        # self.news_dm.insert_recent_hot_news(recent_hot_news, ts_in_min)
        # # event detection
        # new_event_cluster = self.event_manipulator.find_new_event(news_cluster)
        # all_new_event_related_news, all_new_entity_related_event, all_new_event_info = self.new_event_info_init(new_event_cluster, news_cluster)
        # self.news_dm.insert_event_related_news(all_new_event_related_news, ts_in_min)
        # self.event_dm.insert_entity_related_event(all_new_entity_related_event, ts_in_min)
        # self.event_dm.insert_event_info(all_new_event_info, ts_in_min)
        # self.event_dm.insert_event_latest_info(all_new_event_info, ts_in_min)


if __name__ == '__main__':
    mp = main_panel()
    mp.pipeline_main()