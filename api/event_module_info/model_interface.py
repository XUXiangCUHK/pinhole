# -*- coding: utf-8 -*-
import pymysql
import random
from collections import defaultdict, OrderedDict

class load_model():
    def __init__(self):
        self.model_name = 'event_module_info_model'
        self.model_version = '1.0'
        self.type_module_rank = dict()
        self.type_module_rank[0] = [8,1,2,6,4,3,5]
        self.type_module_rank[1] = [8,3,2,1,6,4,5]
        self.type_module_rank[2] = [8,1,2,6,4,3,5]
        self.type_module_rank[3] = [8,6,1,2,4,3,5]
        self.type_module_rank[4] = [8,4,2,1,6,3,5]
        
        self.module_name = dict()
        self.module_name[0] = '瓜田时间线'
        self.module_name[1] = '投票'
        self.module_name[2] = '名家点评'
        self.module_name[3] = '白皮书'
        self.module_name[4] = '各地'
        self.module_name[5] = '瓜田推荐'
        self.module_name[6] = '瓜田百科'
        # self.module_name[7] = '小贴士'
        self.module_name[8] = '最新资讯'
        
    def process(self, input_json):
        event_id = input_json.get('event_id', None)
#        event_type = input_json.get('event_type', None)
        conn,cursor = self.connect_to_database()
        
        event_type = self.get_event_type(cursor, event_id)
        event_module_info = self.get_event_module_info(cursor, event_id, event_type)
        
        output_dict = dict()
        output_dict['event_module_info'] = event_module_info
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

    def get_event_type(self, cursor, event_id):
        event_type_sql = '''
        SELECT event_type FROM melonfield.important_event WHERE event_id = %s
        '''
        cursor.execute(event_type_sql,(event_id,))
        result = cursor.fetchall()
        if len(result)==0:
            return 0
        else:
            return result[0][0]
        
    def get_event_module_info(self,cursor,event_id, event_type):
        event_module_info_sql = '''
            SELECT 
                nm.news_module,
                ern.content_id,
                DATE_FORMAT(nb.publish_time, '%%Y-%%m-%%d %%H:%%i:%%S'),
                na.title,
                na.short_abstract,
                na.abstract,
                na.highlight,
                nb.url,
                nb.source_name
            FROM
                melonfield.event_related_news ern
                    JOIN
                melonfield.news_module nm ON ern.content_id = nm.content_id AND ern.event_id = nm.event_id
                    JOIN
                melonfield.news_abstract na ON ern.content_id = na.content_id
                    JOIN
                streaming.news_basics nb on ern.content_id = nb.content_id
            where ern.event_id = %s order by news_module asc,publish_time desc;      
        '''
        cursor.execute(event_module_info_sql,(event_id,))
        result = cursor.fetchall()
        full_event_module_info = defaultdict(list)
        for row in result:
            module = row[0]
            highlight = row[6].split('|')
            url = row[7]
            if url.startswith('20') or url == 'UNKNOWN':
                url = str()
            # news_info = dict(zip(('content_id','publish_time','title','short_abstract','abstract','highlight'),row[1:]))
            news_info = {
                'content_id': row[1],
                'publish_time': row[2],
                'title': row[3],
                'short_abstract': row[4],
                'abstract': row[5],
                'highlight': highlight,
                'url': url,
                'source_name': row[8]
            }
            full_event_module_info[module].append(news_info)
        
        event_module_info = OrderedDict()
        for module_type in self.type_module_rank[event_type]:
            module_name = self.module_name[module_type]
            if module_type == 1:
                event_module_info[module_name] = dict()
                event_module_info[module_name]['news_info'] = full_event_module_info[module_type]
                if full_event_module_info[module_type]:
                    event_module_info[module_name]['vote_info'] = self.get_vote_question_info(cursor, event_id)
            elif module_type == 5:
                event_module_info[module_name] = self.get_recommendation_info(cursor, event_id)
            else:
                event_module_info[module_name] = full_event_module_info[module_type]
        return event_module_info       

    def get_vote_question_info(self, cursor, event_id):
        vote_question_info_sql = '''
            SELECT
                question_name,
                pos_name,
                neg_name
            FROM
                melonfield.event_vote_question
            WHERE
                event_id = %s;
        '''
        cursor.execute(vote_question_info_sql,(event_id,))
        result = cursor.fetchall()
        vote_info = dict()
        for row in result:
            vote_info = dict(zip(('question_name','pos_name','neg_name'),row[:]))
        if not vote_info:
            vote_info = {'question_name': '对于这个问题你赞成吗？', 'pos_name': '赞成', 'neg_name': '不赞成'}
        vote_info['pos_num'] = random.randint(100,1000)
        vote_info['neg_num'] = random.randint(100,1000)
        return vote_info
    
    def get_recommendation_info(self, cursor, event_id):
        recommendation_info_sql = '''
            SELECT
                rec.recommend_event_id,
                ie.pic_url,
                ie.abstract_title,
                ie.event_type
            FROM
                melonfield.event_recommendation rec
                JOIN
                    melonfield.important_event ie ON rec.recommend_event_id = ie.event_id
            WHERE
                rec.event_id = %s;
        '''
        cursor.execute(recommendation_info_sql,(event_id,))
        result = cursor.fetchall()

        recommendation_info = list()
        for row in result:
            recommendation_info.append(dict(zip(('recommend_event_id', 'pic_url', 'abstract_title', 'event_type'),row[:])))
        return recommendation_info
        