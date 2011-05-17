from iobserver import iPluginError, iPlugin
import shutil
import os.path
import os
import copy

class Replica(iPlugin):
	""" Mirror the watched directory. """
	def __init__(self, *args, **kwargs):
		self._events = {
			'IN_ATTRIB': self._copy_stat,
			'IN_CREATE': self._copy,
			'IN_DELETE': self._delete,
			'IN_MODIFY': self._copy,
			'IN_MOVED_FROM': self._prepare_move,
			'IN_MOVED_TO': self._copy,
			'WATCH_INIT': self._init_mirror,
			'WATCH_DEAD': None,
			'WATCH_RECONFIG': None,
		}
		iPlugin.__init__(self, *args, **kwargs)
	
	def _init_mirror(self, event):
		""" First event ever - do first time sync. """
		try:
			if os.path.exists(self._config['replica_destination']):
				self._delete_target(self._config['replica_destination'])
			shutil.copytree(self._watch.get_path(), self._config['replica_destination'])
		except iPluginError:
			raise
		except (IOError, shutil.Error), data:
			raise iPluginError("Error creating initial mirror: %s" % data)
		except Exception, data:
			raise iPluginError("Unexpected error while creating initial mirror: %s" % data)
		except:
			raise iPluginError("Unexpected error while creating initial mirror.")
	
	def _prepare_move(self, event):
		""" Prepare a move from a MOVED_FROM event. """
		# An object was moved out. Wait to see if the next
		# event that we'll get is going to be an IN_MOVED_TO one.
		self._cache.push('mirror_' + self._watch.get_path(), event)
	
	def _finish_move(self, event, cached_event=None):
		""" A matching MOVED_TO event received - do the move. """
		try:
			source = os.path.join(cached_event.path, cached_event.name)
			source = self._form_destination(source)
			destination = os.path.join(event.path, event.name)
			destination = self._form_destination(destination)
			shutil.move(source, destination)
		except shutil.Error, data:
			raise iPluginError("Error moving '%s' to '%s'." % (source, destination))
	
	def _delete(self, event):
		""" Delete the object specified by the event. """
		target = os.path.join(event.path, event.name)
		target = self._form_destination(target)
		self._delete_target(target)
		
	def _delete_target(self, target):
		""" Delete target file/directory. """
		try:
			if os.path.exists(target):
				if os.path.isdir(target):
					shutil.rmtree(target)
				else:
					os.unlink(target)
			else:
				# We are getting the DELETE event of a file that was actually
				# deleted before we can even recreate it.
				# So do nothing...
				pass
		except Exception, data:
			raise iPluginError("Error deleting '%s': %s" % (target, data))
	
	def _full_path(self, path):
		""" Return normalized and absolute path """
		result = os.path.normpath(path)
		result = os.path.abspath(result)
		return result
	
	def _form_destination(self, path):
		""" Form the target mirror path from the source watched item path. """
		path = self._full_path(path)
		watch_path = self._full_path(self._watch.get_path())
		prefix = os.path.commonprefix([path, watch_path])
		result = None
		while True:
			(dirname, basename) = os.path.split(path)
			if not result:
				result = basename
			else:
				result = os.path.join(basename, result)
			path = dirname
			if path == prefix:
				break
		dest_path = self._config['replica_destination']
		dest_path = self._full_path(dest_path)
		result = os.path.join(self._config['replica_destination'], result)
		return result
	
	def _copy(self, event):
		""" Copy file from watched dir to target mirror """
		source = os.path.join(event.path, event.name)
		destination = self._form_destination(source)
		try:
			if event.is_dir:
				# Don't copy a directory - create it ourselves
				# and copy the metadata ontop
				os.mkdir(destination)
				shutil.copystat(source, destination)
			else:
				shutil.copy2(source, destination)
		#except (shutil.Error, OSError), data:
			#raise iPluginError("Error creating %s: %s." % (destination, data))
		#except Exception, data:
			#raise iPluginError("Unexpected error creating %s: %s." % (destination, data))
		except:
			# Assume error is because we are trying to copy a file that was just deleted.
			# Assume that target dir is writable and noone is messing with it except us.
			# Then the error should be just ignored - the file no longer exists anyway.
			pass
	
	def _copy_stat(self, event):
		source = os.path.join(event.path, event.name)
		destination = self._form_destination(source)
		try:
			shutil.copystat(source, destination)
		except:
			# Again - assume that we failed because source was missing...
			pass
	
	def process_event(self, event):
		if not self._config.has_key('replica_destination'):
			# Bad config!
			raise iPluginError("Missing replica_destination directive.")
			return
		
		if event.event_name == 'WATCH_INIT':
			self._cache.push('mirror_config_' + self._watch.get_path(), self._config, True)
		elif event.event_name == 'WATCH_RECONFIG':
			# Configuration might have changed!
			cached_config = self._cache.get('mirror_config_' + self._watch.get_path())
			if cached_config['replica_destination'] != self._config['mirror_destination']:
				# Our target has changed - reinit
				self._cache.push('mirror_config_' + self._watch.get_path(), self._config, True)
				self._init_mirror(event)
	
		if self._events.has_key(event.event_name):
			# Check if we have a delayed move event:
			cached_event = self._cache.pop('mirror_'+self._watch.get_path())
			if cached_event and event.event_name == 'IN_MOVED_TO' and event.cookie == cached_event.cookie:
				# A matching MOVE event
				self._finish_move(event, cached_event)
				return
			elif cached_event:
				# Not a matching event - object should be deleted
				self._delete(cached_event)
			
			if self._events[event.event_name]:
				self._events[event.event_name](event)
