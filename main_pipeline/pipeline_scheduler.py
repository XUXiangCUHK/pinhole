# -*- coding: utf-8 -*-

import pytz
import logging
from subprocess import Popen
from apscheduler.schedulers.blocking import BlockingScheduler

logging.basicConfig()
logging.getLogger('apscheduler').setLevel(logging.DEBUG)

scheduler = BlockingScheduler()
scheduler.add_job(Popen, 'interval', minutes=30, args=[['python', 'melonfield_pipeline.py']], timezone=pytz.timezone('Asia/Shanghai'))
scheduler.start()
