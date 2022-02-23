# -*- coding: utf-8 -*-
import json
import pymysql
from datetime import datetime,timedelta
from collections import Counter
import requests
from database_related.event_data_manipulator import event_data_manipulator
from database_related.news_data_manipulator import news_data_manipulator
from event_generate.story_timeline_manipulator import story_timeline_manipulator
from melon_module.event_manipulator import event_manipulator
from melon_module.latest_news_module import latest_module
from melon_module.abstract import abstract

class load_model():
    def __init__(self):
        self.model_name = 'frontpage_search_model'
        self.model_version = '1.0'
        self.event_dm = event_data_manipulator()
        self.news_dm = news_data_manipulator()
        latest_event_id = self.event_dm.get_initial_event_id()
        self.event_manipulator = event_manipulator(latest_event_id)
        self.timeline_gen = story_timeline_manipulator()
        self.latest_module = latest_module()
        self.existing_top_key_words = list()
        self.abstract = abstract()
        
    def process(self, input_json):
        conn,cursor = self.connect_to_database()
        key_words = input_json.get('key_words')

        latest_event_id = self.event_dm.get_initial_event_id()
        self.event_manipulator = event_manipulator(latest_event_id)
        search_result = self.search_melon(conn, cursor, key_words)

        output_dict = dict()
        output_dict['search_result'] = search_result
        cursor.close()
        conn.close()
        return output_dict

    def get_version(self):
        return self.model_version

    def get_name(self):
        return self.model_name


    def svc(self,url, input_dict):
        response = requests.post(url=url, json=input_dict)
        return response.json() if response.ok else False

    def connect_to_database(self):
        conn = pymysql.connect(
        host='rm-wz9lh12zwnbo4b457.mysql.rds.aliyuncs.com',port=3306,
        user='melonfield',password='melonfield@DG_2020',
        charset='utf8mb4')
        cursor = conn.cursor()
        return conn,cursor
    
    def search_melon(self, conn, cursor, key_words):
        time_now = datetime.now()
        ts_in_min = time_now.strftime('%Y%m%d%H%M')

        # first find melon in important events, thus melon's title is edited by human
        search_result = dict()
        event_list = self.get_event_list(conn,cursor)
        for event in event_list:
            flag = 1
            for key in key_words:
                if key not in json.loads(event['event_top_word']).get('data'):
                    flag = 0
                    break
            if flag == 1:
                self.existing_top_key_words += json.loads(event['event_top_word']).get('data')[:5]
                print('here is from important event: ', self.existing_top_key_words)
                search_result[event['event_id']] = event['title']
        
        # search inside event_search_word database
        search_event_id_list = list()
        for search_word in key_words:
            search_event_id_list += self.event_dm.get_key_word_event(search_word)
        # regenerate timeline
        # for event_id in search_event_id_list:
        #     self.timeline_gen.generate_story_timeline(event_id, 0, insert_history_flag=False, timeline_runtime_gap = 10080)
        
        key_words_pair_list = list()
        for search_event_id in search_event_id_list:
            key_words_pair = self.event_dm.get_event_key_word_pair(search_event_id)
            search_result[search_event_id] = '和'.join(key_words_pair)
            key_words_pair_list.append(key_words_pair)
        
        # get new key word pairs
        result = self.get_words_pair(conn, cursor, key_words, key_words_pair_list, pair_num=50)
        news_cluster = self.event_manipulator.form_new_event_by_key_words(result)
        all_new_event_related_news, all_new_entity_related_event, all_new_event_info, all_key_words_pair, all_event_id_list, all_event_latest_news, all_delete_info, all_abstract_list = self.new_event_info_init(news_cluster)
        self.news_dm.insert_event_related_news(all_new_event_related_news, ts_in_min)
        self.event_dm.insert_entity_related_event(all_new_entity_related_event, ts_in_min)
        self.event_dm.insert_event_info(all_new_event_info, ts_in_min)
        self.event_dm.insert_event_latest_info(all_new_event_info, ts_in_min)
        self.event_dm.insert_event_search_word(all_key_words_pair)
        self.news_dm.delete_from_news_module(all_delete_info)
        self.news_dm.insert_latest_news_into_news_module(all_event_latest_news)
        self.news_dm.insert_news_abstract(all_abstract_list)
        
        for event_id in all_event_id_list:
            self.timeline_gen.generate_story_timeline(event_id, 0, insert_history_flag=False, timeline_runtime_gap = 43200)
        
        for item in all_new_event_info:
            event_id = item['event_id']
            search_word = list()
            for pair in all_key_words_pair:
                if event_id == pair['event_id']:
                    search_word.append(pair['search_word'])
            title = '和'.join(search_word) + '事件'
            # title = item['abstract_title']
            search_result[event_id] = title

        formatted_search_result = list()
        for event_id, event_title in search_result.items():
            formatted_search_result.append({
                'event_id': event_id,
                'title': event_title
            })
        self.existing_top_key_words = list()
        return formatted_search_result
    
    def ES_search(self, keywords, operator, minimum_should_match, only_title, start_time, end_time):
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
    
    def get_words_pair(self, conn, cursor, key_words, existing_pair_list, pair_num=5):
        search_id_list = self.ES_search(key_words, 'and', 1, True, '2020-01-01', '2021-12-31')
        fetch_news_attribution_dict = dict()
        total_tf_counter = Counter()
        for content_id in search_id_list:
            tf_per_news = self.get_news_attribution(conn, cursor, content_id)
            total_tf_counter += Counter(tf_per_news)

        potential_words = [[x[0].split('|')[0]] for x in total_tf_counter.most_common(pair_num)]
        print('here is potential words:', potential_words)
        for x in key_words:
            try:
                potential_words.remove([x])
            except:
                print('words not in the list')

        search_result = list()
        for potential_pair in potential_words:
            if potential_pair[0] in self.existing_top_key_words:
                continue

            if self.check_entity(conn, cursor, potential_pair[0]) > 0:
                potential_pair[0:0] = key_words
                flag = 1
                for existing_pair in existing_pair_list:
                    if len(set(potential_pair).difference(set(existing_pair))) == 0:
                        flag = 0
                        break
                if flag:
                    search_id_list = self.ES_search(potential_pair, 'and', 1, True, '2020-01-01', '2021-12-31')
                    if len(search_id_list) > 5:
                        search_result.append((potential_pair, len(search_id_list), search_id_list))

        search_result.sort(key=lambda tup: tup[1], reverse = True)
        for item in search_result:
            print('here is search result: ', item[0], item[1])
        return search_result

    def get_event_list(self, conn, cursor):
        event_list_sql = '''
        SELECT 
            ie.event_id,
            ie.abstract_title,
            eli.event_top_word
        FROM
            melonfield.important_event ie
                JOIN
            melonfield.event_latest_info eli 
                ON 
            eli.event_id = ie.event_id
        ORDER BY eli.ts_in_min DESC;
        '''
        cursor.execute(event_list_sql)
        result = cursor.fetchall()
        event_list = [dict(zip(('event_id','title','event_top_word'),x)) for x in result]
        return event_list
    
    def get_news_attribution(self, conn, cursor, content_id):
        statement = "SELECT tf_content FROM melonfield.filtered_news_info WHERE content_id = '{}';".format(content_id)
        cursor.execute(statement)
        select_query_result = cursor.fetchall()
        if select_query_result:
            tf_content = json.loads(select_query_result[0][0])
            return tf_content
        else:
            return Counter()
    
    def check_entity(self, conn, cursor, entity_name):
        statement = "SELECT entity_id from melonfield.entity_info_old where entity_name = '{}';".format(entity_name)
        cursor.execute(statement)
        select_query_result = cursor.fetchall()
        check = -1
        for item in select_query_result:
            check = item[0]
        return check
    
    def new_event_info_init(self, news_cluster):
        all_new_event_related_news = list()
        all_new_entity_related_event = list()
        all_new_event_info = list()
        all_key_words_pair = list()
        all_event_id_list = list()
        all_event_latest_news = list()
        all_delete_info = list()
        all_abstract_list = list()
        for event_id, cluster_info in news_cluster.items():
            content_id_list = cluster_info['content_id_list']
            key_words = cluster_info['key_words']
            print(key_words[1], self.existing_top_key_words)
            if key_words[1] in self.existing_top_key_words:
                continue

            event_news_pool = self.news_dm.get_news_info_from_list(content_id_list)
            event_related_news, entity_related_event, event_info = self.event_manipulator.new_event_info_init(event_id, cluster_info, event_news_pool)
            
            event_latest_news, delete_info = self.latest_module.event_generate_latest_news(event_id, event_news_pool)
            event_updated_news = dict()
            event_updated_news[event_id] = content_id_list
            abstract_list = self.abstract.get_event_latest_valid_news_abstract(event_latest_news, event_updated_news, event_news_pool)
            self.existing_top_key_words += [x.split('|')[0] for x in json.loads(event_info[0]['event_top_word']).get('data')[:5]]
            all_event_latest_news += event_latest_news 
            all_delete_info += delete_info
            all_abstract_list += abstract_list
            all_event_id_list.append(event_id)
            all_new_event_related_news += event_related_news
            all_new_entity_related_event += entity_related_event
            all_new_event_info += event_info
            for search_word in key_words:
                all_key_words_pair.append({
                    'event_id': event_id,
                    'search_word': search_word
                })
        return all_new_event_related_news, all_new_entity_related_event, all_new_event_info, all_key_words_pair, all_event_id_list, all_event_latest_news, all_delete_info, all_abstract_list