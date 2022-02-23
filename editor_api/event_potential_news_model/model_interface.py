# -*- coding: utf-8 -*-
import pymysql
from datetime import datetime,timedelta
from simi_filter import simi_filter
from collections import defaultdict,Counter
from gensim import similarities

class load_model():
    def __init__(self):
        self.model_name = 'event_potential_news_model'
        self.model_version = '1.0'
        self.sf = simi_filter()
        
    def process(self, input_json):
        event_id = input_json.get('event_id')
        page = input_json.get('page')
        conn,cursor = self.connect_to_database()
        
        data = self.get_data(cursor, event_id)
        if len(data)==0:
            filtered_news = list()
        else:
            filtered_news = self.get_offline_result(data, page)
            
        output_dict = dict()        
        output_dict['filtered_news'] = filtered_news
        cursor.close()
        conn.close()
        return output_dict

    def get_version(self):
        return self.model_version

    def get_name(self):
        return self.model_name

    def connect_to_database(self):
        conn = pymysql.connect(
        host='rm-wz9lh12zwnbo4b457.mysql.rds.aliyuncs.com',port=3306,
        user='melonfield',password='melonfield@DG_2020',
        charset='utf8mb4')
        cursor = conn.cursor()
        return conn,cursor       

    def get_data(self, cursor, event_id):
        select_sql = '''
            SELECT 
                nb.content_id,title,nb.publish_time,count(publish_id),word2vec
            FROM
                melonfield.event_latest_info eli
                    JOIN
                melonfield.event_related_news ern ON eli.event_id = ern.event_id
            		JOIN
            	streaming.news_basics nb on ern.content_id = nb.content_id
                   JOIN
            	streaming.news_publish_relation npr on ern.content_id = npr.content_id
            		JOIN
            	streaming.news_word2vec nv on ern.content_id = nv.content_id
            WHERE
                eli.event_id = %s group by nb.content_id order by nb.publish_time;
        '''
        cursor.execute(select_sql,(event_id, ))
        result = cursor.fetchall()
        return result

    def get_offline_result(self, data, page):
        news_collection = list()
        news_pool  = defaultdict(dict)
        for row in data:
            content_id = row[0]
            news_collection.append(content_id)
            title = row[1]
            repost_num = int(row[3])
            w2v = row[4].split(',')
            c=0
            corpus = list()
            for i in w2v:
                corpus.append((c,float(i)))
                c+=1
            publish_time = row[2]
            news_pool[content_id]['title'] = title
            news_pool[content_id]['publish_time'] = publish_time
            news_pool[content_id]['corpus'] = corpus
            news_pool[content_id]['repost_num'] = repost_num
        
        potential_leaf,latest_news_list = self.sf.filter_simi_news(news_collection,news_pool)
        filtered_news_list_sorted = (potential_leaf + latest_news_list)
        filtered_news_list_sorted.reverse()
        start = (page-1) * 10
        end = start + 10
        filtered_news = list()
        for i in filtered_news_list_sorted[start:end]:
            filtered_news_info = dict()
            filtered_news_info['content_id'] = i
            filtered_news_info['title'] = news_pool[i]['title']
            filtered_news_info['publish_time'] = news_pool[i]['publish_time'].strftime('%Y-%m-%d %H:%M:%S')
            filtered_news.append(filtered_news_info)
        
        return filtered_news
