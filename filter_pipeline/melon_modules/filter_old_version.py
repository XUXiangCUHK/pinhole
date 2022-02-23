# -*- coding: utf-8 -*-

# This file provides filter function
# Input: news basic info
# Output: True or False

import re

class Filter:
    def __init__(self):
        self.phrase_pattern_list = ['投资者提问', '审查案件', '律师接单', '发起索赔', '鹰眼预警', '异动股揭秘', '证券代码']
        self.phrase_notice_list = ['公司公告', '今日停复牌公告', '结果公告', '预披露公告', '决议公告', '路演公告', '中签率公告', '计划公告', '进展公告']
        self.phrase_daily_list = ['周报', '日报', '日志', '年报', '月报', '季度报', '季报', '路演']
        self.phrase_finance_list = ['减持', '业绩预告', '股份有限公司', 'ST', '净利润', '持股', '净利同比', '的批复', '融资']
        self.phrase_stock_list = ['操作建议', '走势分析', '升逾', '跌逾', '升近', '跌近', '大盘震荡', '盘中异动', '高位震荡', '强势', '盘中', '走强', '板块']
        self.eliminate_source = ['金融界', '问董秘', '智能写手', 'e公司', '格隆汇', '猪价格网', '食品伙伴网', '中国养猪网', '99期货网', '鸡病专业网', '中关村在线', '金谷高科', '瑞达期货', 
                            '财华社', 'zhtg', '我的钢铁网', '中国粮油信息网', '大越期货', '长安期货', '中国花生信息网', '中钢网', '挖贝', '阿思达克财经新闻', '观点地产网', 
                            '中国饮料行业网', '金投网', '民航资源网', '国家统计局网站', '数据宝', '电车之家网', '农资招商网', '中国石化新闻网', '中国辣椒网', '中果网',
                            '中投投资咨询网', '水产养殖网', '爱投顾股票', '中国煤炭资源网', '山西证券', '我的有色网', '电商报', '中国电池联盟', '中金网']
        self.eliminate_punctuation = ['【', '】', '[', ']', '|', '｜']
        self.elimination_list = self.phrase_pattern_list + self.phrase_notice_list + self.phrase_daily_list + self.phrase_finance_list + self.phrase_stock_list + self.eliminate_punctuation

        self.stock_and_id_re = re.compile(r'[\u4e00-\u9fa5]{3,4}[\(（]\d{5,6}[\)）][\u4e00-\u9fa5]{0,6}：')
        self.stock_only_re = re.compile(r'([\u4e00-\u9fa5]{3,4})：')
        self.date_re = re.compile(r'[（(]{0,1}\d{1,2}月\d{1,2}日[）)]{0,1}')
    
    def contain_chinese(self, text):
        for char in text:
            if '\u4e00' <= char <= '\u9fa5':
                return True
        return False
    
    def news_filter(self, source, title, content=str()):
        if not self.contain_chinese(title):
            return False
        if len(title) <= 5:
            return False
        if source in self.eliminate_source:
            return False
        for phrase in self.elimination_list:
            if phrase in title:
                return False

        if '主动' in title and '盘' in title:
            False
        if '报' in title and '元' in title and '.' in title:
            False
        if source == '新浪' and ('大涨' in title or '大跌' in title) and len(title)<=6:
            False
        if source == '新浪' and ('专利' in title and '排行榜' in title):
            False
        if source == '新浪' and '跟踪' in title:
            False

        if re.findall(self.stock_and_id_re, title):
            return False
        if re.findall(self.date_re, title):
            return False
        if re.match(self.stock_only_re, title):
            stock = re.match(self.stock_only_re, title).group(1)
            if len(stock) == 4 and ('股' in stock or '集团' in stock or '银行' in stock or '业' in stock or '科技' in stock):
                return False
        
        # predict_label = news_classifier_api(title, content=str())
        # if int(predict_label) not in useful_news_label:
        #     return False
        return True