# -*- coding: utf-8 -*-

__author__='lizhengxu'

import logging
import time
import django
from django.db import transaction
from django.db.models import Q
from django.db.models import F

from db.models import ZeusOmg
from thread_cleaner import OmgThreadCleaner
from thread_collector import OmgThreadCollector

from domob_thrift.omg_types.ttypes import *
from domob_thrift.omg_types.constants import *
from domob_thrift.omg.ttypes import *
from domob_thrift.omg.constants import *
from domob_thrift.omg import OmgService

from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer

from elasticsearch import Elasticsearch
from elasticsearch.exceptions import NotFoundError
from elasticsearch.client import IndicesClient
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
		self.collector_on = config.get('default', 'thread_creative_collector.enable')

		self.es_index = config.get('elasticsearch', 'index')
		self.es_type = config.get('elasticsearch', 'type')
		self.es_analyzer = config.get('elasticsearch', 'analyzer')
		self.es_timeout = config.getint('elasticsearch', 'timeout')
		self.es_hosts = self.config.get('elasticsearch', 'hosts').strip().split(",")

		self.imageteller_host = self.config.get('server', 'imageteller.host')
		self.imageteller_port = self.config.getint('server', 'imageteller.port')

		self.imageteller_transport = None
		self.imageteller_client = None

		django.db.connection.cursor().execute('set wait_timeout=3600')

	def initImagetellerClient(self, host, port):
		transport = TSocket.TSocket(host, port)
		transport = TTransport.TFramedTransport(transport)
		protocol = TBinaryProtocol.TBinaryProtocol(transport)
		self.imageteller_client = OmgService.Client(protocol)
		transport.open()
		self.imageteller_transport = transport

	def initClient(self):
		self.initImagetellerClient(self.imageteller_host, self.imageteller_port)

		self.es_client = Elasticsearch(hosts=self.es_hosts)
	
	def closeClient(self):
		if self.imageteller_transport is not None:
			try:
				self.imageteller_transport.close()
				self.imageteller_transport = None
			except:
				pass

	def run(self):
		self.runbackthread()
		self.runAnalyzer()
	
	def runbackthread(self):
		# 收集线程
		if self.collector_on == "on":
			self.collector = OmgThreadCollector(self.config)
			self.collector.daemon = True
			self.collector.start()
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

				self.analyze()
			except:
				self.logger.exception('analyzer error')
			finally:
				self.closeClient()

			if self.running:
				time.sleep(self.interval)

	def analyze(self):
		"""监控

		把上次检测之后的所有状态变化拿出来，发送报警
		"""

		self.logger.info('>>> begin analyze image <<<')
		creatives = ZeusOmg.objects.filter(translated=0)[:10]
		self.logger.info('get %d creative to analyze', len(creatives))

		for creative in creatives:
			tagstr = str()
			try:
				# 从图片识别服务中获取标签和描述
				imageData = ImageData()
				imageData.image_url = creative.image_url
				imageAnalyzeResult = self.imageteller_client.analyzeImage(ImageDataType.IDT_URL, imageData, ImageAnalyzeLanguage.IAL_EN)

				self.logger.debug("request [%s] and imageteller_client return %s", creative.image_url, imageAnalyzeResult)

				if len(imageAnalyzeResult.tags) == 0 and len(imageAnalyzeResult.descriptions) == 0:
					creative.translated = 2
					creative.save()
					continue

				# 结果构成 tag
				for imageTag in imageAnalyzeResult.tags:
					tagstr += imageTag.tag + ' '
				for description in imageAnalyzeResult.descriptions:
					tagstr += description + '. '

				# 通过es的分词功能处理tags
				analyzeRes = self.es_client.indices.analyze(
					index=self.es_index, analyzer=self.es_analyzer,
					text=tagstr
					)

				tags = set()
				for token in analyzeRes['tokens']:
					tags.add(token['token'])

				
				# 如果已存在相同文案，则添加新tags
				sourceRes = None
				try:
					sourceRes = self.es_client.get_source(
						index=self.es_index, doc_type=self.es_type,
						id=hashlib.md5(creative.creative_text).hexdigest()
						)
				except NotFoundError:
					sourceRes = None

				# 说明找到了相同hash
				if sourceRes is not None:
					if sourceRes['mesg'] == creative.creative_text:
						# 可能不同文案出现了相同哈希，理论上还是存在这种可能的
						# 那么如果实际文案是不同的，我们就放弃之前的文案和标签，保存最新的
						# 所以如果text的字符串比较也确实相同了，那么我们就把之前的tag也加上，再做更新
						for tag in sourceRes['tags'].split():
							tags.add(tag)
					else:
						# 虽然hash相同，但字符串比较不同，那么就冲突了，我们放弃之前的记录
						self.logger.warn("[%s] and [%s] have same md5, keep later", sourceRes['mesg'], creative.creative_text)


				tagstr = string.join(tags)

				# 更新es的记录
				self.es_client.index(
					index=self.es_index, doc_type=self.es_type,
					body={
						"tags": tagstr,
						"mesg": creative.creative_text
					},
					id=hashlib.md5(creative.creative_text).hexdigest(),
					request_timeout=self.es_timeout
					)
			except:
				self.logger.error('id:[%d], creative_id:[%d], text:[%s], image_url:[%s], tags:[%s]', creative.id, creative.creative_id, creative.creative_text, creative.image_url, tagstr)
				self.logger.exception('analyze creative error')
				continue

			self.logger.debug('id:[%d], creative_id:[%d], text:[%s], image_url:[%s], tags:[%s]', creative.id, creative.creative_id, creative.creative_text, creative.image_url, tagstr)

			# 走到这里代表已经识别图片并保存了tags和文案到es，那么可以标记这条记录为已经识别
			creative.translated = 1
			creative.save()

			time.sleep(self.analyze_interval)

	def stop(self):
		self.running = False
