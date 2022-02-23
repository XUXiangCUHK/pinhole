#-*- encoding:utf-8 -*-

# import sys
# try:
#     reload(sys)
#     sys.setdefaultencoding('utf-8')
# except:
#     pass

from textrank4zh import TextRank4Keyword, TextRank4Sentence

# tr4w = TextRank4Keyword()
# tr4w.analyze(text=text, lower=True, window=2)  # py2中text必须是utf8编码的str或者unicode对象，py3中必须是utf8编码的bytes或者str对象

# print( '关键词：' )
# for item in tr4w.get_keywords(20, word_min_len=1):
#     print(item.word, item.weight)

# print( '关键短语：' )
# for phrase in tr4w.get_keyphrases(keywords_num=20, min_occur_num= 2):
#     print(phrase)

class Highlight():
    def __init__(self):
        self.tr4s = TextRank4Sentence()
    
    def get_highlight_sent(self, text, sen_num=2):
        self.tr4s.analyze(text=text, lower=True, source = 'all_filters')

        highlight_list = list()
        for item in self.tr4s.get_key_sentences(num=sen_num):
            if item.index == 0:
                continue
            else:
                highlight_list.append(item.sentence)
        return '|'.join(highlight_list)

if __name__ == '__main__':
    H = Highlight()
    content = '''参考消息网10月8日报道据路透社华盛顿10月6日报道。美国大选的提前投票数据显示。在11月前投票的美国人数量创下新高。这暗示特朗普总统与民主党竞选对手拜登之间的这场对决，选民投票率可能创下纪录高位。目前距离11月3日的大选还有四周时间。根据“美国选举计划”统计的提前投票数据。超过380万美国人已经投票，远超过2016年同期的约7.5万人。初期投票人数的激增促使麦克唐纳预计。本次大选的总投票人数将约为1.5亿人。占合格选民总数的65%，为1908年以来的最高值。6日，美国俄亥俄州辛辛那提的选民在排队等待提前投票。'''
    sen = H.get_highlight_sent(content, sen_num=4)
    print(sen)