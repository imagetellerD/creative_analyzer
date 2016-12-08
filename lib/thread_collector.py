# -*- coding: utf-8 -*-

__author__='lizhengxu'

import json
import threading
import time
import logging
import string

import django
from django.db import transaction
from db.models import ZeusOmg

from elasticsearch import Elasticsearch

class OmgThreadCollector(threading.Thread):
	"""删除已经解译的创意
	
	"""

	def __init__(self, config):
		"""初始化

		"""
		threading.Thread.__init__(self)
		self.logger = logging.getLogger('omg.analyzer.collector')
		self.processed_creative_id = 0

		self.config = config
		self.es_index = config.get('elasticsearch', 'index')
		self.es_analyzer = config.get('elasticsearch', 'analyzer')
		self.es_hosts  = self.config.get('elasticsearch', 'hosts').strip().split(",")

	def run(self):
		while True:
			self.logger.info('omg creative collector start ...')
			django.db.close_old_connections()

			self.es_client = Elasticsearch(hosts=self.es_hosts)

			insertRows = 0
			try:
				insertRows = self.thread_collector()
			except:
				self.logger.exception("omg_thread_collector error")
			if insertRows > 0:
				# 发现新的 creative，那么10秒后接着找，避免频繁对数据库造成压力
				time.sleep(10)
			else:
				# 没有找到新的 creative，那么十分钟后再查找
				time.sleep(600)

	@transaction.atomic
	def thread_collector(self):
		cursor = django.db.connection.cursor()

		# 获取已经处理过的最大 creative_id
		sql = "SELECT max(creative_id) as creative_id FROM zeus_omg"
		cursor.execute(sql)

		max_creative = self.dictfetchall(cursor)
		if len(max_creative) != 1:
			self.logger.error("Get max_creative_id from zeus_omg failed, return [%d] rows", len(max_creative))
			return 0

		# 第一次启动collector，那么 zeus_omg表为空，所以这时从 creative_id 最小开始收集即可
		if max_creative[0]['creative_id'] is None:
			max_creative[0]['creative_id'] = 0

		# 获取500个未处理的 creative，保存到 zeus_omg
		sql = '''
		SELECT id, image_id, image_ids, text_id, text_ids 
		FROM zeus_creative 
		WHERE id > %s 
		ORDER BY id 
		LIMIT 500
		'''

		max_creative_id = 0
		if self.processed_creative_id > int(max_creative[0]['creative_id']):
			# 有可能第一次跑collector，然后前面大部分是脏数据，那么会一直循环前面的100条
			max_creative_id = self.processed_creative_id
		else:
			max_creative_id = int(max_creative[0]['creative_id'])
		cursor.execute(sql, (max_creative_id))
		creatives = self.dictfetchall(cursor)
					
		self.logger.debug("collector find %d new creative", len(creatives))
		if len(creatives) == 0:
			return 0

		for creative in creatives:
			# 我们目前只保存英文文案(zeus_creative_text_lib language = en-us)
			# image_id < 1000 时，我们可以认为是多图
			if creative['image_id'] < 1000:
				# 这种是脏数据，跳过
				if creative['image_ids'].strip() == "" or creative['text_ids'].strip()=="":
					self.logger.warn("creative_id:[%s] have dirty data image_ids:[%s], text_ids:[%s]", creative['id'], creative['image_ids'], creative['text_ids'])
					# 表示我们已经处理过了这个id
					self.processed_creative_id = int(creative['id'])
					continue

				imageIds = json.loads(creative['image_ids'])
				textIds = json.loads(creative['text_ids'])

				if len(imageIds) != len(textIds):
					self.logger.warn("creative_id:[%s] have wrong number of image_ids:[%s], text_ids:[%s]", creative['id'], imageIds, textIds)
					# 表示我们已经处理过了这个id
					self.processed_creative_id = int(creative['id'])
					continue

				imageStr = ', '.join(str(x) for x in imageIds)
				textStr = ', '.join(str(x) for x in textIds)

				sql = "SELECT image_url FROM zeus_image_lib WHERE id in (%s) order by find_in_set(id, '%s')"%(imageStr, imageStr)
				cursor.execute(sql)
				urls = self.dictfetchall(cursor)

				sql = "SELECT text, language FROM zeus_creative_text_lib WHERE id in (%s) order by find_in_set(id, '%s')"%(textStr, textStr)
				cursor.execute(sql)
				texts = self.dictfetchall(cursor)

				if len(urls) != len(imageIds) or len(texts) != len(imageIds):
					self.logger.warn("creative_id:[%s] have wrong number of image_ids:[%s], text_ids:[%s], urls:[%s], texts:[%s]", creative['id'], imageIds, textIds, urls, texts)
					# 表示我们已经处理过了这个id
					self.processed_creative_id = int(creative['id'])
					continue

				params = ()

				for index in range(len(texts)):
				#	if texts[index]['language'] != 'en-us' or self.contain_chinese(texts[index]['text']):
					# 发现脏数据略多，通过分词后是否只包含英文和数字来判断是否是英文文案
					if texts[index]['language'] != 'en-us' or not self.is_english(texts[index]['text']):
						self.logger.debug("creative_id:[%s], image_id:[%s], text_id:[%s], image_url:[%s], text:[%s], language:[%s] ignored", 
											creative['id'], imageIds[index], textIds[index], urls[index]['image_url'], texts[index]['text'], texts[index]['language'])
						# 表示我们已经处理过了这个id
						self.processed_creative_id = int(creative['id'])
						continue

					params += ((creative['id'], urls[index]['image_url'], texts[index]['text'], 0),)

				if len(params) != 0:
					sql = "INSERT INTO zeus_omg(creative_id, image_url, creative_text, translated) VALUES (%s, %s, %s, %s)"
					cursor.executemany(sql, params)
			else:
				sql = "SELECT image_url FROM zeus_image_lib WHERE id = %s"
				cursor.execute(sql, (creative['image_id']))
				url = self.dictfetchall(cursor)
				if len(url) != 1:
					self.logger.warn("creative_id:[%s] image_id:[%s] does not exist", creative['id']. creative['image_id'])
					# 表示我们已经处理过了这个id
					self.processed_creative_id = int(creative['id'])
					continue

				sql = "SELECT text, language FROM zeus_creative_text_lib WHERE id = %s"
				cursor.execute(sql, (creative['text_id']))
				text = self.dictfetchall(cursor)
				if len(text) != 1:
					self.logger.warn("creative_id:[%s] text_id:[%s] does not exist", creative['id'], creative['text_id'])
					# 表示我们已经处理过了这个id
					self.processed_creative_id = int(creative['id'])
					continue

			#	if text[0]['language'] != 'en-us' or self.contain_chinese(text[0]['text']):
				# 发现脏数据略多，通过分词后是否只包含英文和数字来判断是否是英文文案
				if text[0]['language'] != 'en-us' or not self.is_english(text[0]['text']):
					self.logger.debug("creative_id:[%s], image_id:[%s], text_id:[%s], image_url:[%s], text:[%s], language:[%s] ignored", 
										creative['id'], creative['image_id'], creative['text_id'], url[0]['image_url'], text[0]['text'], text[0]['language'])
					# 表示我们已经处理过了这个id
					self.processed_creative_id = int(creative['id'])
					continue


				params = ()
				params += ((creative['id'], url[0]['image_url'], text[0]['text'], 0),)
				sql = "INSERT INTO zeus_omg(creative_id, image_url, creative_text, translated) VALUES (%s, %s, %s, %s)"
				cursor.executemany(sql, params)

			# 表示我们已经处理过了这个id
			self.processed_creative_id = int(creative['id'])

		return len(creatives)

	def dictfetchall(self, cursor):
		"Return all rows from a cursor as a dict"
		columns = [col[0] for col in cursor.description]
		return [
			dict(zip(columns, row))
			for row in cursor.fetchall()
		]

	def contain_chinese(self, check_str):
		for ch in check_str.decode('utf-8'):
			if u'\u4e00' <= ch <= u'\u9fff':
				return True
		return False

	def is_english(self, text):
		if text.strip() == "":
			return False
		# 通过es的分词功能处理 descriptionstr
		analyzeRes = self.es_client.indices.analyze(
			index=self.es_index, analyzer=self.es_analyzer,
			text=text
			)

		tags = set()
		for token in analyzeRes['tokens']:
			tags.add(token['token'])

		for tag in tags:
			for ch in tag.decode('utf-8'):
				if '!' <= ch <= '~':
					continue
				else:
					return False
		return True
