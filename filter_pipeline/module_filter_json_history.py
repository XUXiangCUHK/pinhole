import json
from melon_modules.main_filter import Filter
from melon_modules.preprocess import Preprocess
from melon_modules.entity import Entity
from melon_modules.database import Database
from collections import Counter
from datetime import datetime, timedelta

FILTER = Filter()
DB = Database()
NP = Preprocess()
NER = Entity()

time_record = datetime(2020, 11, 16, 16, 20)
while True:
    try:
        start_time_str, end_time_str = DB.get_time_interval(end_time=time_record, gap=10)
        print(start_time_str, end_time_str)
        statement = '''
                    SELECT
                        b.content_id, b.publish_time, b.source_name, b.title, r.content
                    FROM
                        streaming.news_basics b
                        JOIN
                        streaming.news_basics_raw r ON b.content_id = r.publish_id
                        JOIN
                        streaming.news_related_tag t ON b.content_id = t.content_id
                    WHERE
                        (b.insert_time >= '{}' and b.insert_time < '{}') 
                        AND
                        (t.tag_id = '1299900000' or t.tag_id = '1200600000')
                        AND
                        (t.rank = 1)
                        ;
                    '''.format(start_time_str, end_time_str)
        select_query_result = DB.read_from_mysql(statement)

        news_preprocess_result = list()
        filtered_news_info = list()
        filtered_news_entity = list()

        for item in select_query_result:
            content_id = item[0]
            publish_time = item[1]
            source = item[2]
            title = item[3]
            content = item[4]
            
            if FILTER.news_filter_black_list(source, title):
                segged_title, segged_content = NP.preprocess_api(title, content)
                news_preprocess_result.append({
                    'content_id': content_id,
                    'segged_title': json.dumps({'data': segged_title}, ensure_ascii=False),
                    'segged_content': json.dumps({'data': segged_content}, ensure_ascii=False)
                })

                seg_title_cleaned = NP.words_filter(segged_title, take_noun=False, rm_sw=True, rw_fsw=False)
                seg_content_cleaned = NP.words_filter(segged_content, take_noun=False, rm_sw=True, rw_fsw=False)
                TF_title = NP.count_TF(seg_title_cleaned, NP.most_common, normalize=False)
                TF_content = NP.count_TF(seg_content_cleaned, NP.most_common, normalize=True)
                TF_per_news = TF_title + TF_content
                top_words_list_per_news = NP.get_top_words_list(TF_per_news, NP.most_common)
                repost_num = DB.get_repost_num(content_id)

                filtered_news_info.append({
                    'publish_time': publish_time,
                    'content_id': content_id,
                    'tf_title': json.dumps({'|'.join(k): TF_title[k] for k in TF_title}, ensure_ascii=False),
                    'tf_content': json.dumps({'|'.join(k): TF_content[k] for k in TF_content}, ensure_ascii=False),
                    'top_word_per_news': json.dumps({'data': top_words_list_per_news}, ensure_ascii=False),
                    'repost_num': repost_num
                })
                
                title_entity_list = NER.get_entities_list(title)
                for entity in set(title_entity_list):
                    entity_id = NER.check_entity_status(entity)
                    filtered_news_entity.append({
                        'content_id': content_id,
                        'entity_id': entity_id,
                        'title_flag': 1,
                        'count': Counter(title_entity_list)[entity]
                    })

        DB.insert_news_preprocess_result_json(news_preprocess_result)
        DB.insert_filtered_news_info_json(filtered_news_info)
        DB.insert_filtered_news_entity_json(filtered_news_entity)
    except:
        pass

    time_record = time_record - timedelta(minutes=10)
    if time_record < datetime(2020, 1, 1):
        break