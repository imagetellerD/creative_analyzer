# -*- coding: utf-8 -*-

import logging
import time
import django
from django.db import transaction
from django.db.models import Q
from django.db.models import F

from db.models import ZeusOmg
from thread_cleaner import OmgThreadCleaner

from elasticsearch import Elasticsearch
import string
import hashlib


# UnicodeEncodeError: 'ascii' codec can't encode characters in position 0-7: ordinal not in range(128)
# 为了避免contain_chinese函数转码时出现如上错误，需要添加以下三行
import sys
reload(sys)
sys.setdefaultencoding("utf-8")

class OmgAnalyzer(object):
	def __init__(self, config):
		"""初始化

		设置数据库连接的wait_timeout是一个小时
		"""
		self.logger = logging.getLogger('omg.analyzer')
		self.config = config
		self.interval = config.getint('default', 'check.interval.second')
		self.running = True
		self.analyze_interval = config.getint('default', 'analyze.interval.second')

		self.cleaner_on = config.get('default', 'thread_analyzed_creative_cleaner.enable')

		self.es_index = config.get('elasticsearch', 'index')
		self.es_type = config.get('elasticsearch', 'type')
		self.es_analyzer = config.get('elasticsearch', 'analyzer')
		self.es_timeout = config.get('elasticsearch', 'timeout')

		django.db.connection.cursor().execute('set wait_timeout=3600')

	def initMicrosoftClient(self, host, port):
		self.microsoft_client = None

	def initCloudSightClient(self, host, port):
		self.cloud_sight_client = None
	
	def initClient(self):
		self.initMicrosoftClient(
				self.config.get('server', 'microsoft.host'),
				self.config.getint('server', 'microsoft.port')
		)
		self.initCloudSightClient(
				self.config.get('server', 'cloud.sight.host'),
				self.config.getint('server', 'cloud.sight.port')
		)

		hoststr = self.config.get('elasticsearch', 'hosts')
		self.esClient = Elasticsearch(hosts=hoststr.strip().split(","))
	
	def closeClient(self):
		'''
		self.microsoft_client.close()
		self.cloud_sight_client.close()
		'''
		pass

	def run(self):
		self.runbackthread()
		self.runAnalyzer()
	
	def runbackthread(self):
		# 清理线程
		if self.cleaner_on == "on":
			self.cleaner = OmgThreadCleaner()
			self.cleaner.daemon = True
			self.cleaner.start()
	
	def runAnalyzer(self):
		while self.running:
			try:
				# 注意，这里使用短连接，避免服务端重启造成本服务的长期不可用
				# 如果遇到服务端的故障，则忽略这一轮检查，sleep之后再试
				self.initClient()
				django.db.close_old_connections()

				self.monitor()

				self.closeClient()
			except:
				self.logger.exception('analyzer error')

			if self.running:
				time.sleep(self.interval)

	def dictfetchall(self, cursor):
		"Return all rows from a cursor as a dict"
		columns = [col[0] for col in cursor.description]
		return [
			dict(zip(columns, row))
			for row in cursor.fetchall()
		]

	def monitor(self):
		"""监控

		把上次检测之后的所有状态变化拿出来，发送报警
		"""

		self.logger.info('>>> begin analyze image <<<')
		creatives = ZeusOmg.objects.filter(translated=0)[:10]
		self.logger.info('get %d creative to analyze', len(creatives))

		for creative in creatives:
			chineseText = True
			tagstr = str()
			if not self.contain_chinese(creative.creative_text):
				# 出于历史原因，数据处理时，我们无法完全过滤中文文案，这里加判断
				chineseText = False
				try:
					'''
					# 从图片识别服务中获取标签和描述
					m_tags, m_description = self.microsoft_client.request(creative.image_url)
					c_tags, c_description = self.cloud_sight_client.request(creative.image_url)

					# 结果构成 tag
					tagstr = string.join(m_tags) + string.join(c_tags) + string.join(m_description) + string.join(c_description)


					# 通过es的分词功能处理tags
					analyzeRes = self.esClient.indices.analyze(
						index=self.es_index, analyzer=self.es_analyzer,
						text=tagstr
						)

					tags = set()
					for token in analyzeRes['tokens']:
						tags.add(token['token'])

				
					# 如果已存在相同文案，则添加新tags
					sourceRes = None
					try:
						sourceRes = self.esClient.get_source(
							index=self.es_index, doc_type=self.es_type,
							id=hashlib.md5(creative.creative_text).hexdigest()
							)
					except NotFoundError:
						sourceRes = None

					if sourceRes is not None and sourceRes['mesg'] == creative.creative_text:
						# 可能不同文案出现了相同哈希，理论上还是存在这种可能的
						# 那么如果实际文案是不同的，我们就放弃之前的文案和标签，保存最新的
						for tag in sourceRes['tags'].split():
							tags.add(tag)


					tagstr = string.join(tags)

					# 更新es的记录
					self.esClient.index(
						index=self.es_index, doc_type=self.es_type,
						body={
							"tags": tagstr,
							"mesg": creative.creative_text
						},
						id=hashlib.md5(creative.creative_text).hexdigest(),
						request_timeout=self.es_timeout
						)
					'''
					pass
				except:
					self.logger.error('id:[%d], text:[%s], image_url:[%s], chinese_text:[%s], tags:[%s]', creative.id, creative.creative_text, creative.image_url, chineseText, tagstr)
					self.logger.exception('analyze creative error')
					continue

			self.logger.debug('id:[%d], text:[%s], image_url:[%s], chinese_text:[%s], tags:[%s]', creative.id, creative.creative_text, creative.image_url, chineseText, tagstr)
			'''
			# 走到这里代表已经识别图片并保存了tags和文案到es，那么可以标记这条记录为已经识别
			creative.trasnlated = 1
			creative.save()
			'''

			time.sleep(self.analyze_interval)

	def stop(self):
		self.running = False

	def contain_chinese(self, check_str):
		for ch in check_str.decode('utf-8'):
			if u'\u4e00' <= ch <= u'\u9fff':
				return True
		return False
