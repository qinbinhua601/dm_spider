#!/usr/bin/env python
#coding:utf-8
import logging
import logging.handlers
import time
import os
def getNow():
    return time.strftime('%Y-%m-%d-%H-%M-%S',time.localtime(time.time()))
#create folder for logs
DIR = '/usr/local/spider/damai/damai_log/'
if os.path.exists(DIR):
    pass
else:
    os.makedirs(DIR)
log_name= 'qlog'
LOG_FILE = DIR + getNow() + '.log'

handler = logging.handlers.RotatingFileHandler(LOG_FILE, maxBytes = 2*1024*1024, backupCount = 5) # 实例化handler 
# fmt = '%(asctime)s - line:%(lineno)s - %(name)s \t\t\t\t\t %(message)s'
fmt = '%(message)s'

formatter = logging.Formatter(fmt)   # 实例化formatter
handler.setFormatter(formatter)      # 为handler添加formatter

logger = logging.getLogger(log_name)    # 获取名为tst的logger
logger.addHandler(handler)           # 为logger添加handler
def qPrint(msg):
	try:
		print(msg)
		logger.warning(msg)
	except Exception as err:
		print(err)
		logger.error(err)
