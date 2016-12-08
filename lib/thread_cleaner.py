# -*- coding: utf-8 -*-

__author__='lizhengxu'

import threading
import time
import logging

import django
from django.db import transaction
from db.models import ZeusOmg

class OmgThreadCleaner(threading.Thread):
	"""删除已经解译的创意
	
	"""

	def __init__(self):
		"""初始化

		"""
		threading.Thread.__init__(self)
		self.logger = logging.getLogger('omg.analyzer.cleaner')

	def run(self):
		while True:
			self.logger.info('omg translated cleaner start ...')
			try:
				self.thread_cleaner()
			except:
				self.logger.exception("omg_thread_cleander error")
			time.sleep(600) # 每10分钟检查一次

	@transaction.atomic
	def thread_cleaner(self):
		# 我们保留100个最新记录，用以识别保存到哪个creative_id，以便collector可以从未识别creative开始收集
		cursor = django.db.connection.cursor()
		sql = '''
		DELETE FROM zeus_omg 
		WHERE translated = 1 AND creative_id < 
		(SELECT creative_id FROM (SELECT max(creative_id)-100 AS creative_id FROM zeus_omg) AS tmp)
		'''
		cursor.execute(sql)
		transaction.commit_unless_managed()
