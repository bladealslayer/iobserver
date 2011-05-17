from iobserver import iPlugin, iPluginError
import os.path
import datetime

class Scribe(iPlugin):
	""" The logging plugin """
	
	# Here is a dictionary of the messages we print.
	_messages = {
	# file|directory '<name>' was accessed.
	'IN_ACCESS': "%s '%s' was ACCESSED",
	# the metadata of file|dir '<name>' was changed
	'IN_ATTRIB': "The METADATA for %s '%s' was changed",
	'IN_CLOSE_NOWRITE': "%s '%s' was CLOSED without been written to",
	'IN_CLOSE_WRITE': "%s '%s' was CLOSED",
	'IN_CREATE': "%s '%s' was CREATED",
	# file|directory '<name>' was deleted.
	'IN_DELETE': "%s '%s' was DELETED",
	'IN_DELETE_SELF': "watched %s '%s' was itself DELETED",
	'IN_MODIFY': "%s '%s' was MODIFIED",
	'IN_MOVE_SELF': "watched %s '%s' was itself MOVED",
	'IN_MOVED_FROM': "%s '%s' just MOVED OUT",
	'IN_MOVED_TO': "%s '%s' just MOVED IN",
	'IN_OPEN': "%s '%s' was OPENED",
	'IN_foo': "",
	'WATCH_INIT': "WATCH STARTED",
	'WATCH_DEAD': "WATCH STOPPED",
	}
	
	def _log(self, msg):
		if not self._config.has_key('scribe_log'):
			raise iPluginError("Missing scribe_log directive.")
			return
		
		if self._config['scribe_log'] != '-':
			log_file = self._cache.pop('scribe_' + self._config['scribe_log'])
			if not log_file:
				try:
					log_file = file(self._config['scribe_log'], 'a')
					self._cache.push('scribe_' + self._config['scribe_log'], log_file, True)
				except IOError, data:
					raise iPluginError("Could not open log file '%s': %s" % (self._config['scribe_log'], data))
			
			try:
				print >>log_file, "%s " % datetime.datetime.now() + msg
			except IOError, data:
				raise iPluginError("Could not write to log file '%s': %s" % (self._config['scribe_log'], data))
		else:
			# 'scribe_log = -' means write to stdout
			print "%s " % datetime.datetime.now() + msg
	
	def process_event(self, event):
		watch = self._watch
		cache = self._cache
		
		try:
		
			message = self._messages[event.event_name]
			
			if event.event_name.startswith('WATCH_'):
				self._log(("scribe: %s: " % event.path) + message)
				return
			
			name = event.name
			if not name: name = '.'
			
			if event.is_dir:
				message = message % ('directory', name)
			else:
				message = message % ('file', name)
			
			self._log(("scribe: %s: " % event.path) + message)
				
			if event.event_name.startswith('IN_MOVED_'):
				# Try to find a match in the cache
				cached_event = cache.pop('scribe_'+str(event.cookie))
				if cached_event:
					moved_to = event
					moved_from = cached_event
					if event.event_name == 'IN_MOVED_FROM':
						moved_to, moved_from = moved_from, moved_to
					
					message = "scribe: MOVE events matched: file '%s' was moved to '%s'"
					if event.is_dir:
						message = "scribe: MOVE events matched: directory '%s' was moved to '%s'"
					
					self._log(message % (os.path.join(moved_from.path, moved_from.name), os.path.join(moved_to.path, moved_to.name)))
				
				else:
					# Not found in cache - this is first hit
					cache.push('scribe_'+str(event.cookie), event)	
			
		except KeyError:
			pass
