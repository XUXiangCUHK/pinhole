# -*- coding: utf-8 -*-

import pytz
import logging
from subprocess import Popen
from apscheduler.schedulers.blocking import BlockingScheduler

logging.basicConfig()
logging.getLogger('apscheduler').setLevel(logging.DEBUG)

scheduler = BlockingScheduler()
scheduler.add_job(Popen, 'interval', minutes=10, args=[['python', 'module_filter_json.py']], timezone=pytz.timezone('Asia/Shanghai'))
scheduler.start()
