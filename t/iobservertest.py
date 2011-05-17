import unittest

from time import sleep

from iobserver import *
from iobserver.plugins import scribe, mirror

import os
import os.path

class iObserverTest(unittest.TestCase):
	def testConfig(self):
		""" Test confiration parsing """
		io = iObserver()
		self.assertTrue(io._config['global']['watch_config'] == False)
		
		io = iObserver({'global':{'watch_plugins':True},'watches':{}})
		self.assertTrue(io._config['global']['watch_plugins'] == True)
		self.assertTrue(io._config['global']['watch_config'] == False)
		
		io = iObserver('config')
		self.assertTrue(io._config['global']['watch_config'] == '1')
		self.assertTrue(io._config['watches'][os.path.realpath('watch1')] == {})
		
	
	def testCacheExpire(self):
		""" Test the iCache expire function """
		cache = iCache(0, 10)
		for i in range(1, 6):
			cache.push(i, i)
		self.assertTrue(len(cache._cache) == 5)
		for i in range(1, 6):
			cache.push(i+10, i, True)
		self.assertTrue(len(cache._cache) == 10)
		cache.push('boo', 1) # This should cause the expire to kick in
		self.assertTrue(len(cache._cache) == 6)
		
	def testCacheNoExpire(self):
		""" Test the iCache "persistent" option """
		cache = iCache(3, 10)
		for i in range(1, 6):
			cache.push(i, i)
		self.assertTrue(len(cache._cache) == 5)
		for i in range(1, 6):
			cache.push(i+10, i, True)
		self.assertTrue(len(cache._cache) == 10)
		cache.push('boo', 1) # This should cause the expire to kick in
		self.assertTrue(len(cache._cache) == 11)
		
	def testCache(self):
		""" General ppush/pop test """
		cache = iCache(0, 10)
		value = 666
		key = 'number of the beast'
		cache.push(key, value)
		value = cache.pop(key)
		self.assertTrue(value == 666)
		
	def testWatchBad(self):
		""" Here we have a bad watch config """
		io = iObserver()
		watch = iWatch(io, {}, {'/a/b/c': {'pluginss': ""}})
		self.assertTrue(io.error())
	
	def testWatchGood(self):
		""" Here we have a good config """
		io = iObserver()
		watch = iWatch(io, {'dummy': None}, {'/a/b/c': {'plugins': "dummy"}})
		self.assertFalse(io.error())
	
	def testExceptions(self):
		""" Test error notification """
		io = iObserver()
		io.start()
		iWatchError(io, "TEST")
		sleep(1)
		io.stop()
		sleep(1)
		self.assertTrue(io.error() == "TEST")
	
	def testLogging(self):
		""" Test logging """
		if os.path.exists('test'):
			os.system('rm -rf test/*')
		if os.path.exists('scribe.log'):
			os.unlink('scribe.log')
		io = iObserver()
		watch = iWatch(io, {'scribe': scribe}, {'./test': {'plugins': 'scribe', 'scribe_log': 'scribe.log'}})
		watch.start()
		sleep(1)
		os.system("touch test/foo")
		sleep(1)
		watch.stop()
		sleep(1)
		self.assertTrue(os.path.exists('scribe.log'))
		
		log = file('scribe.log', 'r')
		order = ['STARTED', 'CREATED', 'OPENED', 'METADATA', 'CLOSED', 'STOPPED']
		for line in log:
			self.assertTrue(line.find(order.pop(0)) != -1)
		
	def testMirror(self):
		""" Test mirroring """
		if os.path.exists('test'):
			os.system("rm -rf test/*")
		if os.path.exists('mirrored'):
			os.system("rm -rf mirrored")
		io = iObserver()
		watch = iWatch(io, {'mirror': mirror}, {'./test': {'plugins': 'mirror', 'mirror_destination': 'mirrored'}})
		watch.start()
		sleep(1)
		os.system("touch test/foo")
		sleep(1)
		os.system("mkdir test/blade")
		os.system("touch test/bar")
		os.system("mv test/bar test/blade/bar")
		sleep(1)
		watch.stop()
		sleep(1)
		self.assertTrue(os.path.exists('mirrored/foo'))
	
if __name__ == "__main__":
	unittest.main()
