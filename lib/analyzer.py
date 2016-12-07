#!/usr/local/bin/python
# -*- coding: utf-8 -*-

import argparse
import ConfigParser
import logging.config
import os
import sys
import signal
from domob_pyutils.dm303 import DomobBase

basepath = os.path.realpath(os.path.dirname(__file__)+'/../')
sys.path.append(basepath+'/lib')
sys.path.append(basepath+'/lib/gen-py')
sys.path.append("/home/zeus/lizhengxu/hack/python-lib/lib/python2.7/site-packages")

if __name__ == '__main__':
	ap = argparse.ArgumentParser(description = 'domob omg analyzer')
	ap.add_argument('-d', '--executeDir', type = str,
		help = 'execute directory',
		default = basepath)

	args = ap.parse_args()
	print 'run creative analyzer at %s' % args.executeDir

	os.chdir(args.executeDir)

	logConfFile = args.executeDir+'/conf/logging.conf'
	logging.config.fileConfig(logConfFile)

	cfgfile = args.executeDir + '/conf/analyzer.conf'
	cfg = ConfigParser.RawConfigParser()
	cfg.read(cfgfile)

	sys.path.append(os.path.join(basepath, 'conf'))
	os.environ.setdefault("DJANGO_SETTINGS_MODULE", "django_settings")
	from omg_analyzer import OmgAnalyzer

	oa = OmgAnalyzer(cfg)
	def _stop(s, frame):
		oa.stop()
	signal.signal(signal.SIGTERM, _stop)
	signal.signal(signal.SIGINT, _stop)
	oa.run()
	

