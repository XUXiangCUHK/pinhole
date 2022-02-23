# -*- coding: utf-8 -*-
"""
Created on Mon Nov  2 14:19:23 2020

@author: user
"""

import pymysql
import re
from datetime import datetime, timedelta
import unicodecsv

wRe= re.compile(r'\s+')
type0Re = re.compile(
    r'^.{0,2}(预报|启事|广告|预告|信息|预测)$|(天文|天气).{0,4}(报告|预报|预告|预测|趋势|[阴云雨雷晴雪])|(旅游|城市|全国).{1,6}天气|金牌.{0,6}(户型|楼盘)|楼盘巡礼|抗皱亮眼|[热在]线答疑|(耳聋|鼻炎|面瘫|白发|生病|鼾|耳鸣|风潮|养颜|高血压|癌|痛风|肿瘤|中风|糖尿病|前列腺|骨质疏松|盘突出).*(为何|奇药|热捧|咨询|习惯|专家|怎么|怎样|如何|特效|什么|窍门|神秘|神奇|秘诀|诀窍|惊喜|治好|好了|巧治|克星|难治|新突破|皇家|秘方|秘密|不再)|(为何|巧治|热捧|奇药|习惯|特效|咨询|专家|如何|怎样|怎么|窍门|神秘|神奇|诀窍|惊喜|治好|好了|什么|不再|克星).*(耳聋|这么好|鼾|白发|耳鸣|鼻炎|高血压|风潮|癌|痛风|肿瘤|养颜|生病|面瘫|中风|秘方|秘密|皇家|糖尿病|骨质疏松|盘突出)|(?:[^迷]|^)你|她|您|[有相]约(?!束)|买齐|生活资讯|灰甲不灰心|好礼|(养生|健康|庆|迎|超值|时尚|金秋|寒冬|优惠|免费|新春|节).*(大礼|礼包|套餐|礼品|礼物)|(大礼|礼包|大礼|礼包|套餐|礼品|礼物).*(优惠|节|免费|养生|健康|庆|迎|金秋|寒冬|新春)|有奖.{1,4}问答|[保护补][肝肾]|耳聋耳鸣|车商车事|就白了|.{0,2}接.{0,3}版.{0,2}|连明星都|租售转让|优惠首选|房产出售|有问有答|商报分类|节目单$|直播表$|招募$|大募集$|抗癌明星|广告[:：\-—]|(?:标题|图片)新闻|[(（]广告[）)]|健康.{0,2}(讲座|论坛|速递|快递|资讯|窍门|诀窍|常识)|missing document|^\S{0,2}(?:节目|网页|网|网站|版面|今日|最新|每日|每周|今天|电影|新闻|电视|文娱|明日|直播|栏目|旅游|旅行|明星|网上)(?:[a-z]+|推荐|指引|目录|咨询|索引|遥控器|踪横|关注|集粹|焦点|预告|指南|情报|纵横|导读|热点|资讯|看点)|([每本当一今昨明\d][日晨周晚早月]).{1,3(电影|微博|博客|微信|热点|节目|电视|栏目)|^(无标题|it研讨会|最新影片|公司简介|社团信息|理财人生|折起$|物品信息$|有乜睇|数字真相|破天荒|热线快报|全明星投顾)$|(积分榜|美容志|名将酒|享免费法律谘询|解答|热线|节目表|围脖星语|人气榜|好戏一周|我想知道|分类信息|停电|收益榜|报名方式|遗失声明|星踪|更正|(生活|市民|热线|便民|民生)(热线|服务站)|万事通|早知道|采风|战绩|这样说|导读|串串烧|导视|速读|导览|速览|汇报|集锦|精彩|集萃|便利通|公告栏|专家信箱|追星指南|包打听|(市场|股市).{0,4}备忘录|上映电影|及时帖|启示|贴士|大补帖|互动易|战报|留言板|传真|同城分类|直通车|税务天地|movies|events|问答)$|(招生|租房|售房|房源|招聘|招工)(广告|启事|信息)|享.{0,6}(礼包}优惠|折扣)|活动.讲座|一句话新闻|新闻回放|上东网知天下|大唐双龙传|更正与说明|(中天|卫视).{1,3}台|3分钟CBN|(体坛|体育|体讯)(快递|快讯|速递|短讯|简讯|摘要|传真)|寻找目击证人|新闻.递|猜中了吗|品牌婚介|哪里玩|诚征|征订|星座|运程|运势|名家相马|分类信息|有事大家说|追踪大小非|中房·御翠园$|知道有着数|猜猜猜|体坛关键词|新闻搜索|今日一条娱|赢马笔记|必看好戏|创业板点兵|中关村进行曲|历史上的今天|美眸|人言可“味”|本期明星产品|武汉新兰德|新闻集装箱|爣棰|开放的编前会|深晚悦读女郎|一周降价优惠|一周大事件|一周保险信息|时报体育频道|无风不起浪|一览表|pk台|赛事望远镜|跟我学|刮大奖|^(新闻发布会|市场二传手|新股中签号|高院入禀状)$|楼盘检索|(比赛|赛事|演出).*(预告|资讯|看点|精彩)|更正与澄清|精彩瞬间|图片新闻|商业车险.{1,2}省|限量(版|销售)|^想.+[?？]|巧治|分钟cbn|沥青挂牌价$|客运信息$|活动启事$|([^寻]宝|卖|价格)行情$|巡[展礼]$|^(怎么|怎样|如何|快来)|从.*做起|^(天啊|震惊)[!！,，]|畅玩|[每本后当一今昨明\d][日天晨周晚早月波轮]?迎?(暴雨|气温|降雨|降水|降温)')
type1Re = re.compile(r'公告$|(报告|大会|决议).{0,2}(摘要|纪要|精选|通知|决议)|股东大会$|(年度|季度|中期|财务)报告$|说明书$|通函$|征询函$|意见书$|公司声明$|财务指标$')
type2Re = re.compile(
    r"((股市|股票|基金|债券|证券|板块|债市|深证|恒指|沪指|综指|财经|新股|理财|早盘|盘前|操作|[年中季]报|个股|市况|公告|报告|行情|上证|尾盘|大盘|午盘|早市|午市|晚间|[每本当一今昨明\d][日晨周晚早月]|市场|交易|公司信息)[^，,。；]{0,2}(停复牌|专递|汇总|信息|盘点|回顾|综述|总结|速递|提示|要闻|推荐|精点|清点|短波|导航|行情|传真|快讯|短讯|简讯|指南|摘要|备忘|动态|动向|提醒|扫描|一览|概览))|(沪市|深市|a股|净值|增长|换手率|收入|支出|流入|流出|亏损|利润|净利|成交|幅|率|额|量|涨|跌|降|增|减)[^，,。，；]{0,2}前[\d一二三四五六七八九十]+|(沪市|深市|a股|净值|增长|换手率|收入|支出|流入|流出|亏损|利润|净利|成交|幅|率|额|量|涨|跌|降)[^，,。，；]{0,2}[\d一二三四五六七八九十排]+[大强名]|[日周月]报$|累计涨幅|上交所信息|深交所信息|行情可(期|关注)|上海证券交易所|深圳证券交易所|[aA][hH]股价比|公司(快递|速递)|(炒手|名家|伯乐|专家|轮商|机构|券商|基金|散户|股评家)[^，,。，；]{0,2}(兵法|把脉|看好|推荐|新进|重仓|点评|盘点|总结|提示|荐股|精选)|热点板块|股票池|限售股解禁|证券投资基金|交易提示|市场收益率|速览|金股|短讯|牛熊证|荐股|必读|选股|评股|评级股票|基金资金|[每本当一今昨明][日周月](提示|债市|股市|市场|债券|权证|新股|大事|要闻)|(新高|低价|高派现|高送转|新低|强势|潜力|热门|人气|黑马|热|热点|异动|白马|题材|绩优|牛气|牛|优质|活跃)的?个?股|海外股票市场|荐股大观|行业走势图|[买选]股要|风险警示|大宗交易|外汇·债券|市场表现|港交所上市公司|群英会|[a、h港]股价比|(深证|上证|恒指|沪指|综指)\D{1,6}[\d一二三四五六七八九十]+|看盘|看市|点评|日志|评述|(特别|强烈|重点|股)推荐|(强于|跑赢)(市场|大市|大盘)|精彩回顾|(股民|股票|股市|股票|个股)[^，,。，；]{0,4}(问诊|诊所|门诊|答疑|咨询|热线|诊断|把脉)|精选$|提示$|信息快递|^(上|深)证指数$|电子邮件专递|^.{0,3}[热在]线$|(主力|板块|资金)动向|(个股|行情)[^，,。，；]{0,2}分化|汇总$|公司(信息|消息|动向|简讯|短讯|快讯|快递|动态|速递|盘点|摘要)|评级简报|操盘工作室|炒股看图表|^(开放式基金|申银万国证券|封闭式基金|网上营业部)$|信息烽火台|fun析$|股排行$|[^上落黑入]榜$|前[一二三四五六七八九]?[一二三四五六七八九十]名?$|擂“股”鸣金|([股市]|基金)(预告|排行|排名|汇报)$|交易看台$|[看上][空多涨跌](股票|个股|板块|行业)$|瘾股市橱窗瘳|长线[^，,。，；]{0,2}白龙马|股市三剑客|[每本当一今昨明\d][日晨周晚早月][^，,。，；]{0,3}(权证|基金)$|股价敏感消息|联合证券邱晨|资金流向$|股票之最|股市[^，,。，；]{0,2}分钟|股事儿|上市定位$|汇牌价$|策略王$|基金净值|关注[^，,。，；]*股票|(的股票|个股)$|[最前后][^，,。，；]*只股票|^看[^，,。，；]*微信|[^，,。，；]*家[^，,。，；]*公司[^，,。，；]*[年中季财]报|.*布局[^，,。，；]*(行情|股|板块)$|(资金|机构|基金)(布局|关注)[^ ]*(板块|股|概念|行业)|(板块|a股|b股|美股|股市|大盘|大市|概念|[沪深道股国板期]指)[^ ][涨跌升降]|反弹行情|博反弹|买新股|多空|[长短]线|量能|市场热情|.*(权证|轮|股|沪|深市|指|期货)[^，,。，； ]*成交|成交[^ ]*[缩放]|(抄底|结构性)[^ ]*(机会|时机)|大市[^场]|净流[出入]|金流[出入向]|前[^ ]*[\d一二三四五][0十]股|种走势|筹码集中|筹码分散|大盘|盘口|[次再续股点空].*(探底|上攻|下探)|(探底|上攻|下探).*[次再续股点空]|(?<!(人事|风暴))震荡|黑色星期|板指数|[大中小]盘股|连[阴阳]|失守|股活跃|融资客|分化|基金抱团|反弹新高|挑战[^ ]*点|流入量|流出量|超跌|逢低|逢高|布局[^ ]*不晚|赶底|[补止][涨跌升]|^(留意|关注)[^ ]*[股板]|[^老平]板齐|牛熊|慢牛|容易赚钱|板[^ ]*轮转|货如轮转|揭秘[^ ]*持仓|修正估值")
reChinese = re.compile(r"[\u4E00-\u9FFF]+", re.IGNORECASE)
reRepeat = re.compile(r'^[每本当一今昨明\d][日晨周晚早月]')
reRegular = re.compile(r'融资净|融资融券信息|融资余额|日盘..[幅停]|停.{1,3}报于|盘中(最高|快速)|日开盘|日[快加]速|日打开.停|律师接单|股东户数.{4,20}户均持股|报价|收盘行情|早上开盘行情|板块(活跃|强势|走强|[涨跌]幅)|[价格调降涨锁]{2}信息|^.{3,5}大涨$|龙虎榜|收于年线之上')

class academic_internet_filter:
    def __init__(self):
#        self.valid_source_id_set = set()
#        with open('网络新闻过滤选取源.csv','rb') as f:
#            info = unicodecsv.reader(f)
#            for row in info:
#                source_id = int(row[0])
#                self.valid_source_id_set.add(source_id)
        self.szrds_filtered_table = 'streaming.news_basics'
        self.szrds_origin_table = 'streaming.news_basics_raw'
        self.hkserver_filtered_table = 'aiden_temp.news_origin_record'
        self.hkserver_origin_table = 'aiden_temp.news_publish_record'        
        
    def get_type(self, title):
        """
        根据匹配的模式来判断
        """        
        typeRes = ((type0Re, 0), (type1Re, 1), (type2Re, 2), (reRegular, 9))
        
        title = wRe.sub("", title.lower())
        if reChinese.search(title):
            tag = 3
            for typeRe, ty in typeRes:
                #            print typeRe
                if typeRe.search(title):
                    tag = ty
                    break
        else:
            tag = 4
        if tag == 3 and len(title) < 5 or (len(title) < 7 and reRepeat.search(title)):
            tag = 5
        return tag        

    def connect_to_szrds(self):
        conn = pymysql.connect(
        host='rm-wz9lh12zwnbo4b457ro.mysql.rds.aliyuncs.com',port=3306,
        user='weichu',password='weichu2018',
        charset='utf8mb4')
        cursor = conn.cursor()
        return conn,cursor  

    def connect_to_hkserver(self):
        conn = pymysql.connect(
        host='192.168.1.8',port=3810,
        user='weichu',password='Datago,2019',
        charset='utf8mb4')
        cursor = conn.cursor()
        return conn,cursor   
    
    def select_news_from_database(self, cursor, filtered_table, origin_table, start_time, end_time):
        statement = '''
        SELECT a.content_id, a.publish_time, a.source_id, a.source_name, a.url, a.title, b.content, a.platform, a.process_id FROM {} a join {} b on a.content_id = b.publish_id
        WHERE a.platform!='papr' and a.source_id != -1 and a.publish_time >= '{}' and a.publish_time < '{}';
        '''.format(filtered_table, origin_table, start_time, end_time)
        cursor.execute(statement)
        data = cursor.fetchall()
        return data
    
    def insert_news_into_hkserver(self, cursor, conn, data):
        statement = '''
        INSERT IGNORE INTO NEWS_RESULTS.internet_news_for_academic 
        (`publish_time`, `process_id`, `content_id`, `source_id`, `source_name`,`platform`, `url`, `title`,`content`)
        VALUES
        (%(publish_time)s, %(process_id)s, %(content_id)s, %(source_id)s, %(source_name)s, %(platform)s, %(url)s, %(title)s, %(content)s);
        '''
        cursor.executemany(statement, data)
        conn.commit()
        
    def filter_news(self, data):
        filtered_data = list()
        for row in data:
            source_id = row[2]
            source_name = row[3]
            if source_name == '智能写手' or source_id == 4:
                continue
#            if source_id not in self.valid_source_id_set or source_name == '智能写手':
#                continue
            title = row[5]
            tag = self.get_type(title)
            if tag!=3:
                continue
            data_dict = dict()
            data_dict['content_id'] = row[0]
            data_dict['publish_time'] = row[1]
            data_dict['source_id'] = source_id
            data_dict['source_name'] = source_name
            data_dict['url'] = row[4]
            data_dict['title'] = title
            data_dict['content'] = row[6]
            data_dict['platform'] = row[7]
            data_dict['process_id'] = row[8]
            filtered_data.append(data_dict)
        return filtered_data
    
    def main(self, start_time, end_time):
        if start_time < datetime(2015,1,1):
            while start_time < datetime(2015,1,1) and start_time < end_time:
                limit_time = start_time + timedelta(days=14)
                if limit_time >= datetime(2015,1,1):
                    limit_time = datetime(2015,1,1)
                if limit_time >= end_time:
                    limit_time = end_time
                conn, cursor = self.connect_to_hkserver()
                data = self.select_news_from_database(cursor, self.hkserver_filtered_table, self.hkserver_origin_table, start_time, limit_time)
                filtered_data = self.filter_news(data)
                print(start_time, limit_time, len(filtered_data))
                self.insert_news_into_hkserver(cursor, conn, filtered_data)
                start_time = limit_time
                cursor.close()
                conn.close()
        
        while start_time < end_time:
            limit_time = start_time + timedelta(days=1)
            if limit_time >= end_time:
                limit_time = end_time
            hkserver_conn, hkserver_cursor = self.connect_to_hkserver()
            szrds_conn, szrds_cursor = self.connect_to_szrds()
            data = self.select_news_from_database(szrds_cursor, self.szrds_filtered_table, self.szrds_origin_table, start_time, limit_time)
            filtered_data = self.filter_news(data)
            print(start_time, limit_time, len(filtered_data))
            self.insert_news_into_hkserver(hkserver_cursor, hkserver_conn, filtered_data)
            start_time = limit_time
            szrds_cursor.close()
            szrds_conn.close()    
            hkserver_cursor.close()
            hkserver_conn.close()
    
    def test_filter(self, start_time, end_time):
        conn, cursor = self.connect_to_szrds()
        data = self.select_news_from_database(cursor, self.szrds_filtered_table, self.szrds_origin_table, start_time, end_time)
        filtered_data = self.filter_news(data)
        with open('check.csv','wb') as output:
            writer = unicodecsv.writer(output)
            for row in filtered_data:
                writer.writerow([row.get('title')])
        
#        with open('trash_check.csv','wb') as output:
#            writer = unicodecsv.writer(output)
#            for row in trash_bin:
#                writer.writerow([row])
        
        cursor.close()
        conn.close()
        return 0

aci = academic_internet_filter()
start_time = datetime(2009,1,1)
end_time = datetime(2020,10,1)
aci.main(start_time, end_time)

