import pandas as pd
import streamlit as st
from collections import defaultdict
import random
import ast

import SessionState

from utils import potential_event, event_potential_news, news_abstract, cluster_abstract, baidu_abstract
from utils import edit_news, edit_news_module, edit_event, edit_side_note, awaken_melon
from utils import add_vote_question, add_event_recommendation, edit_timeline, fetch_highlight
from utils import event_timeline, event_module, find_melon_by_key_words, generate_melon_by_key_words, generate_timeline


state = SessionState.get(rerun=False, name="",
                         data=[0, None, None, None, None],  # news type, headline, short abstract, complete abstract
                         bn={0: False, 1: False, 2: False, 3: False, 4: False,
                             5: False, 6: False, 7: False, 8: False, 9: False},
                         selected_melons=None, melon_id=None, melon_name=None, melon_url=None,
                         question=None, pos_name=None, neg_name=None,
                         recomm_lst=[])

state.user_name = "melon_editor_tools"


class EditorTools(object):
    """
    EditorTools class provides a visualized tool for platform users to select and edit news abstract
    """

    def __init__(self):
        st.sidebar.markdown("# Melon Editor Tools")
        self.option = st.sidebar.selectbox("",
                                           ("Find Melons", "Search and Generate Melons", "Edit A Melon",
                                            "Add Sticky Notes"))

        if self.option == "Find Melons":
            self.page_find_melons()
        elif self.option == "Edit A Melon":
            self.page_edit_a_melon()
        elif self.option == "Search and Generate Melons":
            self.page_search_and_generate_melons()
        else:
            self.page_add_sticky_notes()

    def page_find_melons(self):
        """
        Display all machine detected melons to help editor select quality ones

        :return: list(event nums): user selected melons
        """
        st.title("Find Melons")

        st.write("Display all machine detected melons, select ones you want to put on \"*MelonField*\" website")

        st.sidebar.write(" ")
        st.sidebar.write("__Melons are sorted by time__")
        num = st.sidebar.number_input("please input a number to view the melons",
                                      min_value=1, max_value=100, step=1, value=1)
        st.write(potential_event(num))

        st.sidebar.write(" ")
        st.sidebar.write("__Selected melons__")
        state.selected_melons = st.sidebar.text_area("write down the *event_id* here, please use *space* as "
                                                     "a delimiter", state.selected_melons or " ")
        st.sidebar.write("The melons you selected:  \n**{}**".format(state.selected_melons))

        confirmed = st.sidebar.button("Confirm your selection?")
        if confirmed:
            st.sidebar.write("**Success!**  \nPlease go to *Edit A melon* to further modify the melon before sending "
                             "to the website")

    def page_search_and_generate_melons(self):
        st.title("Search Melons")

        st.write("Search melons by *keywords* and *start time*")
        note1 = st.empty()
        search_res = st.empty()

        st.title("Generate Melons")
        st.write("Generate melons by *keywords* and *start time, end time*")
        note2 = st.empty()
        generate_res = st.empty()

        st.sidebar.write(" ")
        st.sidebar.write("__Melons are sorted by time__")
        op = st.sidebar.selectbox("", ("Search melons", "Generate melons"))

        st.sidebar.write("Keywords")
        keywords = st.sidebar.text_input("enter keywords: e.g. 特朗普 疫情")
        st.sidebar.write("Operator")
        operator = st.sidebar.radio("select an operator to act on keywords list", ("and", "or"), index=0)
        or_operator_note = st.sidebar.empty()
        or_operator_input = st.sidebar.empty()
        min_num = 0
        if operator == "or":
            or_operator_note.write("Minimum keywords to match")
            min_num = or_operator_input.number_input("enter a number", min_value=1, step=1, value=1)

        title_only = st.sidebar.selectbox("use title only? ", ("True", "False"))
        st.sidebar.write("Start time")
        start_time = st.sidebar.text_input("enter start time: e.g. 202010011230")
        st.sidebar.write("End time")
        end_time = st.sidebar.text_input("enter end time: e.g. 202010011230")

        gmode = st.sidebar.radio("Use **Evolve** mode? (Be aware that the 'evolve' mode will take a long time to run)",
                                 ("Yes", "No"), index=1)

        kws = [str(k).strip() for k in keywords.split(" ")]
        if st.sidebar.button("Run"):
            if op == "Search melons":
                res = find_melon_by_key_words(kws, int(start_time))
                note1.write("keywords: __{}__, start time: __{}__. Find __{}__ melons".format(kws,
                                                                      start_time[:4]+"-"+start_time[4:6]+"-"+
                                                                      start_time[6:8]+" "+start_time[8:10] +":"+
                                                                      start_time[10:], len(res)))

                search_res.write(res)

            else:
                if gmode == 'Yes':
                    mode = 'evolve'
                else:
                    mode = ''

                res = generate_melon_by_key_words(kws, operator, min_num, ast.literal_eval(title_only),
                                                  int(start_time), int(end_time), mode)

                note2.write("keywords: __{}__, start time: __{}__, end time: __{}__"
                         .format(kws, start_time[:4] + "-" + start_time[4:6] + "-" +
                                 start_time[6:8] + " " + start_time[8:10] + ":" +start_time[10:],
                                 end_time[:4] + "-" + end_time[4:6] + "-" +
                                 end_time[6:8] + " " + end_time[8:10] + ":" + end_time[10:]
                                 ))

                generate_res.write(res)

        st.sidebar.write("---")
        st.sidebar.write("**Generate Timeline**")
        idx = st.sidebar.number_input("enter an event id", min_value=0, step=1, value=1)
        typex = st.sidebar.number_input("enter an event type", min_value=0, step=1, value=1)
        if st.sidebar.button("generate"):
            res = generate_timeline(idx, typex)
            if res == "success":
                st.sidebar.write("Success!")

        st.sidebar.write("---")
        st.sidebar.write("**Awaken a melon**")
        idx = st.sidebar.number_input("enter an event id", min_value=0, step=1, value=1, key='awaken')
        start_time = st.sidebar.text_input("enter start time: e.g. 202010011230", key='awaken')
        if st.sidebar.button("awaken"):
            res = awaken_melon(idx, int(start_time))
            if res == "success":
                st.sidebar.write("Success!")

    def write_news_type_to_db(self, melon_id, news_id):
        res = edit_news_module(melon_id, news_id, state.data[0])
        if res == "success":
            return 1

    def write_news_to_db(self, news_id):
        res = edit_news(news_id, state.data[1], state.data[2], state.data[3], state.data[4])
        if res == "success":
            return 1
        else:
            return 0

    def write_event_to_db(self):
        res1 = edit_event(state.melon_id, state.melon_name, state.melon_url, state.melon_type)
        res2 = add_vote_question(state.melon_id, state.question, state.pos_name, state.neg_name)
        res3 = add_event_recommendation(state.melon_id, state.recomm_lst)
        if (res1 == "success") & (res2 == "success") & (res3 == "success"):
            return 1
        else:
            return 0

    def view_more(self, melon_id, news_id, widgets, dtitle, bs, bl, keys):
        # display edit_form
        state.data[0] = 0
        state.data[0] = widgets[0].number_input("Enter a news type (0: timeline, 1: discussion, 2: comment," 
                                                "3: white paper, 4: location, 5: recommendation, 6: wiki, 7: None,"
                                                "8: latest news, 9: extensive reading)",
                                                min_value=0, max_value=9, step=1, value=state.data[0], key=keys[0])

        state.data[1] = dtitle
        state.data[1] = widgets[1].text_input("edit headline", state.data[1] or " ", key=keys[1])
        state.data[2] = bs
        state.data[2] = widgets[2].text_area("edit one-line abstract", state.data[2] or " ", key=keys[2])
        state.data[3] = bl
        state.data[3] = widgets[3].text_area("edit complete abstract", state.data[3] or " ", key=keys[3])
        state.data[4] = bl
        state.data[4] = widgets[4].text_area("recommended highligh", fetch_highlight(state.data[4])
                                             or " ", key=keys[4])
        if st.button("save", key=keys[5]):
            # if (state.data[1] == dtitle) & (state.data[3] == bl) &\
            #         (state.data[2] == "") & (state.data[0] == 0):
            #     st.write("Nothing changed!")

            if self.write_news_type_to_db(melon_id, news_id) & self.write_news_to_db(news_id):
                st.write("Saved!")
                # st.write("**-----Preview-----**", "  \n*type:*", state.data[0],
                #          "  \n*headline:*", state.data[1],
                #          "  \n*short abstract:*", state.data[2],
                #          "  \n*complete abstract:*", state.data[3], "  \n")

    def load_news_details(self, news_id, melon_id):
        first3lines, abstract, baidu_short, baidu_long = news_abstract(news_id)
        st.write("**abstract1** ", first3lines)
        st.write("**abstract2** ", abstract)
        st.write("**baidu50** ", baidu_short)
        st.write("**baidu150** ", baidu_long)

        return baidu_short, baidu_long

    def page_edit_a_melon(self):
        st.title("Edit A Melon")
        st.write("Edit _melon name, news headline, abstract_ for selected melons!")

        st.sidebar.write("**  \nSelected Melons:**  \n{}".format(state.selected_melons))
        melon_id = st.sidebar.number_input("Input a melon id to edit", value=0, step=1)
        self.clear_melon_state()
        st.sidebar.write("** \nView More News**")
        page = st.sidebar.number_input("Enter a page number", value=1, step=1)

        st.sidebar.write("---")
        st.sidebar.markdown("#### Baidu abstract suggestion")
        nid = st.sidebar.text_input("news id", "")
        alen = st.sidebar.number_input("required abstract length", value=50, step=1)
        if nid:
            baidu_text = baidu_abstract(nid, alen)
            st.sidebar.write(baidu_text)

        st.sidebar.write("---")
        st.sidebar.write("** \nEdit the series**  \n")
        state.melon_id = melon_id
        st.sidebar.write("melon id ", state.melon_id)
        # edit vote module
        st.sidebar.write("---")
        st.sidebar.write("**Vote**")
        state.question = st.sidebar.text_input("add a vote question", "")
        state.pos_name = st.sidebar.selectbox("positive side name", ("赞成", "可以", "同意", "会"))
        state.neg_name = st.sidebar.selectbox("negatvie side name", ("不赞成", "不可以", "不同意", "不会"))
        if st.sidebar.button("Save Vote"):
            res = add_vote_question(state.melon_id, state.question, state.pos_name, state.neg_name)
            if res == 'success':
                st.sidebar.write("Success!")
        st.sidebar.write("---")
        st.sidebar.write("**Recommendations**")
        recomm_lst = st.sidebar.text_input("enter a list of recommendations", "")
        if recomm_lst:
            state.recomm_lst = [int(x) for x in recomm_lst.split(",")]
        if st.sidebar.button("Save Recommendations"):
            res = add_event_recommendation(state.melon_id, state.recomm_lst)
            if res == 'success':
                st.sidebar.write("Success!")

        st.sidebar.write("---")
        st.sidebar.write("**Series**")
        state.melon_name = st.sidebar.text_input("enter a series name", "")
        state.melon_url = st.sidebar.text_input("enter a picture url", "")
        state.melon_type = st.sidebar.number_input("enter a series type. 0:timeline 1:policy 2:opinion 3:character "
                                                   "4:social news/others",
                                                   min_value=-1, max_value=4, value=-1, step=1)
        keywords = st.sidebar.text_input("enter a list of keywords to guide the direction, e.g. 青岛 疫情")
        kws = [str(k).strip() for k in keywords.split(" ")]

        st.write("  \n")
        if st.sidebar.button("Save Series"):
            res = edit_event(state.melon_id, state.melon_name, state.melon_url,
                             state.melon_type, ','.join([kw for kw in kws]))
            if res == 'success':
                st.sidebar.write("Success!")
            else:
                st.sidebar.write("whoops, something wrong!")
            # if self.write_event_to_db():
            #     st.sidebar.write("Success!")
            #     self.clear_melon_state()

        filtered = event_potential_news(melon_id, page)
        st.write("**Suggested news: total number {}**".format(len(filtered)))

        widgets = dict()

        try:
            idx = 0
            news_id, title = filtered[idx]['content_id'], filtered[idx]['title']
            st.write(filtered[idx])
            edit_bn = st.button("view more", key=idx)
            if edit_bn:
                state.bn[idx] = True
            if state.bn[idx]:
                bs, bl = self.load_news_details(news_id, melon_id)

                widgets[idx] = [st.empty(), st.empty(), st.empty(), st.empty(), st.empty(), st.empty()]
                self.view_more(melon_id, news_id, widgets[idx], title, bs, bl, ["a"+str(idx), 'b'+str(idx),
                                                                  'c'+str(idx), 'd'+str(idx),
                                                                  'e'+str(idx), 'f'+str(idx)])

            idx = 1
            news_id, title = filtered[idx]['content_id'], filtered[idx]['title']
            st.write(filtered[idx])
            edit_bn = st.button("view more", key=idx)
            if edit_bn:
                state.bn[idx] = True
            if state.bn[idx]:
                bs, bl = self.load_news_details(news_id, melon_id)
                widgets[idx] = [st.empty(), st.empty(), st.empty(), st.empty(), st.empty(), st.empty()]
                self.view_more(melon_id, news_id, widgets[idx], title, bs, bl, ["a" + str(idx), 'b' + str(idx),
                                                                  'c' + str(idx), 'd' + str(idx),
                                                                  'e'+str(idx), 'f'+str(idx)])

            idx = 2
            news_id, title = filtered[idx]['content_id'], filtered[idx]['title']
            st.write(filtered[idx])
            edit_bn = st.button("view more", key=idx)
            if edit_bn:
                state.bn[idx] = True
            if state.bn[idx]:
                bs, bl = self.load_news_details(news_id, melon_id)
                widgets[idx] = [st.empty(), st.empty(), st.empty(), st.empty(), st.empty(), st.empty()]
                self.view_more(melon_id, news_id, widgets[idx], title, bs, bl, ["a" + str(idx), 'b' + str(idx),
                                                                  'c' + str(idx), 'd' + str(idx),
                                                                  'e'+str(idx), 'f'+str(idx)])

            idx = 3
            news_id, title = filtered[idx]['content_id'], filtered[idx]['title']
            st.write(filtered[idx])
            edit_bn = st.button("view more", key=idx)
            if edit_bn:
                state.bn[idx] = True
            if state.bn[idx]:
                bs, bl = self.load_news_details(news_id, melon_id)
                widgets[idx] = [st.empty(), st.empty(), st.empty(), st.empty(), st.empty(), st.empty()]
                self.view_more(melon_id, news_id, widgets[idx], title, bs, bl, ["a" + str(idx), 'b' + str(idx),
                                                                  'c' + str(idx), 'd' + str(idx),
                                                                  'e'+str(idx), 'f'+str(idx)])

            idx = 4
            news_id, title = filtered[idx]['content_id'], filtered[idx]['title']
            st.write(filtered[idx])
            edit_bn = st.button("view more", key=idx)
            if edit_bn:
                state.bn[idx] = True
            if state.bn[idx]:
                bs, bl = self.load_news_details(news_id, melon_id)
                widgets[idx] = [st.empty(), st.empty(), st.empty(), st.empty(), st.empty(), st.empty()]
                self.view_more(melon_id, news_id, widgets[idx], title, bs, bl, ["a" + str(idx), 'b' + str(idx),
                                                                  'c' + str(idx), 'd' + str(idx),
                                                                  'e'+str(idx), 'f'+str(idx)])

            idx = 5
            news_id, title = filtered[idx]['content_id'], filtered[idx]['title']
            st.write(filtered[idx])
            edit_bn = st.button("view more", key=idx)
            if edit_bn:
                state.bn[idx] = True
            if state.bn[idx]:
                bs, bl = self.load_news_details(news_id, melon_id)
                widgets[idx] = [st.empty(), st.empty(), st.empty(), st.empty(), st.empty(), st.empty()]
                self.view_more(melon_id, news_id, widgets[idx], title, bs, bl, ["a" + str(idx), 'b' + str(idx),
                                                                  'c' + str(idx), 'd' + str(idx),
                                                                  'e'+str(idx), 'f'+str(idx)])

            idx = 6
            news_id, title = filtered[idx]['content_id'], filtered[idx]['title']
            st.write(filtered[idx])
            edit_bn = st.button("view more", key=idx)
            if edit_bn:
                state.bn[idx] = True
            if state.bn[idx]:
                bs, bl = self.load_news_details(news_id, melon_id)
                widgets[idx] = [st.empty(), st.empty(), st.empty(), st.empty(), st.empty(), st.empty()]
                self.view_more(melon_id, news_id, widgets[idx], title, bs, bl, ["a" + str(idx), 'b' + str(idx),
                                                                  'c' + str(idx), 'd' + str(idx),
                                                                  'e'+str(idx), 'f'+str(idx)])

            idx = 7
            news_id, title = filtered[idx]['content_id'], filtered[idx]['title']
            st.write(filtered[idx])
            edit_bn = st.button("view more", key=idx)
            if edit_bn:
                state.bn[idx] = True
            if state.bn[idx]:
                bs, bl = self.load_news_details(news_id, melon_id)
                widgets[idx] = [st.empty(), st.empty(), st.empty(), st.empty(), st.empty(), st.empty()]
                self.view_more(melon_id, news_id, widgets[idx], title, bs, bl, ["a" + str(idx), 'b' + str(idx),
                                                                  'c' + str(idx), 'd' + str(idx),
                                                                  'e'+str(idx), 'f'+str(idx)])

            idx = 8
            news_id, title = filtered[idx]['content_id'], filtered[idx]['title']
            st.write(filtered[idx])
            edit_bn = st.button("view more", key=idx)
            if edit_bn:
                state.bn[idx] = True
            if state.bn[idx]:
                bs, bl = self.load_news_details(news_id, melon_id)
                widgets[idx] = [st.empty(), st.empty(), st.empty(), st.empty(), st.empty(), st.empty()]
                self.view_more(melon_id, news_id, widgets[idx], title, bs, bl, ["a" + str(idx), 'b' + str(idx),
                                                                  'c' + str(idx), 'd' + str(idx),
                                                                  'e'+str(idx), 'f'+str(idx)])

            idx = 9
            news_id, title = filtered[idx]['content_id'], filtered[idx]['title']
            st.write(filtered[idx])
            edit_bn = st.button("view more", key=idx)
            if edit_bn:
                state.bn[idx] = True
            if state.bn[idx]:
                bs, bl = self.load_news_details(news_id, melon_id)
                widgets[idx] = [st.empty(), st.empty(), st.empty(), st.empty(), st.empty(), st.empty()]
                self.view_more(melon_id, news_id, widgets[idx], title, bs, bl, ["a" + str(idx), 'b' + str(idx),
                                                                  'c' + str(idx), 'd' + str(idx),
                                                                  'e'+str(idx), 'f'+str(idx)])
        except BaseException as e:
            pass

    def page_add_sticky_notes(self):
        st.title("Add Sticky Notes")
        st.write("Add _'Ke Dai Biao notes', new word entries, short videos_ for selected melons!")

        st.sidebar.write("**  \nSelected Melons:**  \n{}".format(state.selected_melons))
        melon_id = st.sidebar.number_input("Input a melon id to edit", value=0, step=1)

        st.write("**  \nTimeline:**  \n")
        st.write(event_timeline(melon_id))
        try:
            st.write("**  \nOther customized modules:**  \n")
            st.write(event_module(melon_id))
        except:
            pass
        st.sidebar.write("---")
        st.sidebar.write("**Timeline**")
        content_id = st.sidebar.text_input("enter content id")
        mode = st.sidebar.selectbox("action", ("add", "remove"))
        if st.sidebar.button("Save Timeline"):
            res = edit_timeline(melon_id, content_id, mode)
            if res == 'success':
                st.sidebar.write("Success!")

        st.sidebar.write("---")
        st.sidebar.write("** \nAdd Sticky Notes**  \n")
        news_id = st.sidebar.text_input("enter a news id", "")
        note_title = st.sidebar.text_input("add a note title", "")
        note_text = st.sidebar.text_area("add note text", "")
        note_url = st.sidebar.text_area("add note url", "")
        note_type = st.sidebar.number_input("add a note type (0: words, 1: images, 2: videos, "
                                            "3: other websites, 4: new words explanation)",
                                            value=0, step=1, max_value=4)

        if st.sidebar.button("save"):
            if edit_side_note(melon_id, news_id, note_title, note_url, note_text, note_type):
                st.sidebar.write(note_title)
                st.sidebar.write("success!")
            else:
                st.sidebar.write("failed!")

    def clear_melon_state(self):
        state.melon_id = None
        state.melon_name = None
        state.melon_url = None
        state.melon_type = 0


if __name__ == "__main__":
    tools = EditorTools()
