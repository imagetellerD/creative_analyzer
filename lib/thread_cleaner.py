# -*- coding: utf-8 -*-

import threading
import time
import logging
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
		timeout_threads = ZeusOmg.objects.filter(translated=1).delete()
