# -*- coding: utf-8 -*-
"""
Created on Tue Sep 29 17:47:47 2020

@author: user
"""

import json
import pymysql
from contextlib import closing

class database:
    def __init__(self):
        self.mysql_host = 'rm-wz9lh12zwnbo4b457.mysql.rds.aliyuncs.com'
        self.mysql_port = 3306
        self.mysql_user = 'melonfield'
        self.mysql_password = 'melonfield@DG_2020'
    
    def read_from_mysql(self, statement):
        mysql_connection = pymysql.connect(host=self.mysql_host, port=self.mysql_port, user=self.mysql_user, passwd=self.mysql_password)
        with closing(mysql_connection.cursor()) as cursor:
            cursor.execute(statement)
            select_query_result = cursor.fetchall()
        mysql_connection.close()
        return select_query_result

    def write_into_mysql(self, data, statement):
        if data:
            mysql_connection = pymysql.connect(host=self.mysql_host, port=self.mysql_port, user=self.mysql_user, passwd=self.mysql_password)
            with closing(mysql_connection.cursor()) as cursor:
                cursor.executemany(statement, data)
                mysql_connection.commit()
            mysql_connection.close()