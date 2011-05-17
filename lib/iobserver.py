############################################################################
#    iObserver                                                             #
#    v0.1                                                                  #
#                                                                          #
#    Copyright (C) 2007 by Boyan Tabakov                                   #
#    blade.alslayer@gmail.com                                              #
#                                                                          #
#    This program is free software; you can redistribute it and/or modify  #
#    it under the terms of the GNU General Public License as published by  #
#    the Free Software Foundation; either version 2 of the License, or     #
#    (at your option) any later version.                                   #
#                                                                          #
#    This program is distributed in the hope that it will be useful,       #
#    but WITHOUT ANY WARRANTY; without even the implied warranty of        #
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the         #
#    GNU General Public License for more details.                          #
#                                                                          #
#    You should have received a copy of the GNU General Public License     #
#    along with this program; if not, write to the                         #
#    Free Software Foundation, Inc.,                                       #
#    59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.             #
############################################################################

from pyinotify import *
from pyinotify import Event as pyinotify_Event
from configobj import ConfigObj, ConfigObjError

from threading import Thread, Lock, Event
from glob import glob
from types import ModuleType
from time import time

import imp
import plugins

import sys
import os.path
import copy

# Exception classes

# Note that no exceptions are thrown out of our main class
# because we are running in a separate thread and thus the
# exception will be left uncaught.
# Instead the user is able to check if we died with some error.

# Of course, just like every rule, this has one exception :)
# Exception is raised if the error situation occures before
# the thread is started: e.g. config error.

class iError(Exception):
	""" General error """
	def __init__(self, observer, msg):
		Exception.__init__(self, msg)
		if observer:
			observer._notify_error(self)
class iPublicError(iError):
	""" Exceptions that generate error to the user. """
	pass
class iPrivateError(iError):
	""" Exceptions for internal use. """
#class iConfigError(iPublicError):
	#""" Configuration error """
	#pass
class iWatchError(iPublicError):
	""" Error while watching a target """
	pass
class iObserverError(iPublicError):
	""" General observer error """
	pass
class iPluginError(iPrivateError):
	""" Parent class for exceptions originating
	from within plugins. Plugins should derive
	exceptions from this one.
	These are caught by iWatch and are not passed to user.
	An iProcessEventError is raised instead. """
	def __init__(self, *args):
		if len(args) == 1:
			iPrivateError.__init__(self, None, args[0])
		else:
			iPrivateError.__init__(self, *args)


# So it begins...
class iPlugin(object):
	""" Base class for plugins """
	def __init__(self, watch, cache, config):
		self._config = config
		self._cache = cache
		self._watch = watch

	def process_event(self, event):
		""" This is the method that is called to handle an event. """
		pass

class iProcessEvent(ProcessEvent):
	""" Our universal event handler. """
	def __init__(self, watch):
		self._watch = watch
	
	def process_default(self, event):
		""" Do nothing, just pass event to the iWatch instance to handle. """
		self._watch.process_event(event)

class iCache(object):
	""" A stash shared by all watches. Passed to plugins
	to store data in, because plugin objects are not persistent. """
	def __init__(self, max_age, expire_after_count):
		self._cache = {}
		self._max_age = max_age
		self._expire_counter = 0
		self._expire_after_count = expire_after_count
		self._lock = Lock()
	
	def push(self, key, value, persistent=False):
		self._lock.acquire()
		self._expire_counter += 1
		if self._expire_counter > self._expire_after_count:
			# Every N pushes, purge any old entries
			self._expire()
			self._expire_counter = 0
		time_stamp = 0
		if not persistent:
			time_stamp = time()
		self._cache[key] = (value, time_stamp)
		self._lock.release()
	
	def pop(self, key):
		self._lock.acquire()
		(result, time_stamp) = self._cache.pop(key, (None, None))
		self._lock.release()
		return result
	
	def get(self, key):
		self._lock.acquire()
		(result, time_stamp) = self._cache.get(key, (None, None))
		self._lock.release()
		return result
	
	def _expire(self):
		current_time = time()
		for (key, (value, time_stamp)) in self._cache.items():
			if time_stamp and current_time - time_stamp > self._max_age:
				self._cache.pop(key)

class iWatch(object):
	""" Represents a single watched directory.
	Watch the directory in a separate thread
	using the desired plugins. """
	def __init__(self, observer, available_plugins, config):
		self._observer = observer
		self._cache = observer._cache
		self._lock = Lock()
		self._thread = Thread(target=self.run)
		self._terminate_event = Event()
		self._error_event = Event()
		self._config_changed_event = Event()
		self._config = None
		self._path = None
		self._avilable_plugins = None
		self._watch_manager = None
		self._notifier = None
		self._watches = None
		
		self._configure(available_plugins, config)
	
	def get_path(self):
		return self._path
	
	def is_alive(self):
		return self._thread.isAlive()
	
	def _configure(self, available_plugins, config):
		self._available_plugins = available_plugins.copy()
		temp = copy.deepcopy(config)
		self._path = temp.keys()[0]
		self._config = temp[self._path]
		
		# Check plugins
		# Redundant check
		if not self._config.has_key('plugins'):
			self._error_event.set()
			
			iWatchError(self._observer, "Watch %s: Missing 'plugins' section in configuration." % self._path)
			
			return
		
		plugins = self._config['plugins']
		
		if not isinstance(plugins, list):
			plugins = [plugins]
		
		for plugin in plugins:
			if not self._available_plugins.has_key(plugin):
				self._error_event.set()
				iWatchError(self._observer, "Required plugin '%s' is missing." % plugin)
		
	def update_config(self, available_plugins, config):
		""" Calles from iObserver whenever a change of plugins or config
		is detected. """
		
		# Not running in the main thread of the instance!
		
		self._lock.acquire()
		self._new_available_plugins = available_plugins
		self._new_config = config
		self._lock.release()
		self._config_changed_event.set()
	
	def _reconfigure(self):
		""" Called in the main watch thread so that no locking
		of the configuration when reading is required. """
		self._lock.acquire()
		self._configure(self._new_available_plugins, self._new_config)
		self._lock.release()
	
	def start(self):
		if not self._terminate_event.isSet() and not self._error_event.isSet():
			try:
				self._thread.start()
			except:
				self._error_event.set()
				iWatchError(self._observer, "Could not start watch thread.")
	
	def run(self):
		""" Our thread's main executable """
		# Send a custom WATCH_INIT event.
		# Plugins may use this to do any "one time" initializations.
		process_event = iProcessEvent(self)
		
		process_event.process_default(pyinotify_Event(
			{
			'event_name': 'WATCH_INIT',
			'path': self._path
			}
		))
		
		self._watch_manager = WatchManager()
		self._notifier = Notifier(self._watch_manager, iProcessEvent(self))
		try:
			self._watches = self._watch_manager.add_watch(self._path, EventsCodes.ALL_EVENTS, rec=True, auto_add=True)
			for watch in self._watches.keys():
				if self._watches[watch] == -1:
					# Error: path is missing?
					iWatchError(self._observer, "Error watching %s. Maybe file or directory don't exist?" % watch)
					return
			
			# Rock'n'Roll baby!
			while True:
				self._notifier.process_events()
				if self._notifier.check_events(timeout=1000):
					self._notifier.read_events()
				# Check if our config should be updated
				if self._config_changed_event.isSet():
					self._config_changed_event.clear()
					self._reconfigure()
					# Notify plugins that a configuration might be changed.
					# They should act accordingly...
					process_event.process_default(pyinotify_Event(
						{
						'event_name': 'WATCH_RECONFIG',
						'path': self._path
						}
					))
				# Check if we have to terminate:
				if self._error_event.isSet():
					self._terminate_event.set()
				if self._terminate_event.isSet():
					self._terminate_event.clear()
					self._notifier.stop()
					break
			
			# Send a custom final event: WATCH_DEAD
			# A plugin may use this to cleanup anything 
			# left behind in the cache.
			
			process_event.process_default(pyinotify_Event(
				{
				'event_name': 'WATCH_DEAD',
				'path': self._path
				}
			))
		
		except NotifierError, data:
			self._notifier.stop()
			iWatchError(self._observer, "Error while watching %s: %s" % (self._path, data))
		except ProcessEventError, data:
			self._notifier.stop()
			iWatchError(self._observer, "Error processing event while watching %s: %s" % (self._path, data))
		except:
			self._notifier.stop()
			iWatchError(self._observer, "Unknown error while watching %s." % self._path)
	
	def process_event(self, event):
		""" iProcessEvent calls this to handle an event.
		I could have used iWatch as an event handler directly given
		to the notifier, but this way it is a lil' more flexible.
		A side effect is that a watch could be (possibly) used as a plugin -
		plugin for plugin management:) """
		
		# Our job is to call each of the plugins' process_event methods.
		# Plugins are instantiated each time, so that a reloaded plugin
		# could be updated.
		
		# If we are stopped (or rather "stopping", ignore any events:
		# We need to clear the terminate_event in order our
		# final WATCH_DEAD event to pass. This is done in the run method.
		if self._error_event.isSet() or self._terminate_event.isSet():
			return
		
		# Some special handling:
		# If the watched directory is moved - stop watching it,
		# because paths are no longer valid:
		
		if event.event_name == 'IN_MOVE_SELF' and event.path == self._path+'-invalided-path':
			self.stop()
		
		# Also if watched item gets deleted, the internal inotify watch will
		# be stopped, but our thread will still be running... so stop it
		
		if event.event_name == 'IN_DELETE_SELF' and event.path == self._path:
			self.stop()
		
		plugins = self._config['plugins']
		if not isinstance(plugins, list):
			# In case we have a single plugin, it is a string
			# and not a list...
			plugins = [plugins]
		plugins = set(plugins)
		
		for plugin_name in plugins:
			if self._available_plugins.has_key(plugin_name):
				
				plugin_config = dict([(key, self._config[key]) for key in self._config.keys() if key.startswith(plugin_name + '_')])
				
				plugin_class = None
				plugin = None
				if isinstance(self._available_plugins[plugin_name], ModuleType):
					plugin_class = self._available_plugins[plugin_name].__getattribute__(plugin_name.title())
					plugin = plugin_class(self, self._cache, plugin_config)
				else:
					plugin = self._available_plugins[plugin_name]
				
				# Process event
				try:
					plugin.process_event(event)
				except iPluginError, data:
					iWatchError(self._observer, "Watch: %s: Plugin '%s' reported error: %s" % (self._path, plugin_name, data))
					
			else:
				iWatchError(self._observer, "Watch: %s: Required plugin '%s' is missing." % (self._path, plugin_name))
	
	def stop(self):
		self._terminate_event.set()

class iPollWatch(iWatch):
	""" A watch using polling """
	# Used only to watch our config file
	# because vi causes some trouble otherwise.
	# No recursion implemented!
	def run(self):
		if not os.path.isfile(self._path):
			iWatchError(self._observer, "Missing target or target is not a regular file!")
		else:
			last_mtime = os.stat(self._path).st_mtime
			try:
				while True:
					mtime = os.stat(self._path).st_mtime
					if mtime > last_mtime:
						# File was modified
						last_mtime = mtime
						process_event = iProcessEvent(self)
						event = pyinotify_Event(
							{
							'event_name': 'IN_MODIFY',
							'path': self._path,
							'name': None,
							}
						)
						process_event.process_default(event)
				
					self._terminate_event.wait(1)
					if self._terminate_event.isSet():
						break
			except:
				iWatchError(self._observer, "Could not stat target!")

class iObserver(iPlugin):
	""" The main class. Runs in a separate thread. """
	def __init__(self, config=None):
		self._thread = Thread(target=self.run)
		self._config = None
		self._config_path = None
		self._error = None
		self._config_changed_event = Event()
		self._plugins_changed_event = Event()
		self._terminate_event = Event()
		self._error_event = Event()
		self._configure(config)
		self._plugins = None
		self._watches = None
		self._config_watch = None
		self._plugins_watch = None
		self._cache = iCache(max_age=10, expire_after_count=100)
		
		self._load_plugins()
	
	def _notify_error(self, error):
		# An exception rose somewhere in our threads.
		# Maybe terminate or examine error?
		if isinstance(error, iPublicError):
			self._error = error
			self._error_event.set()
	
	def is_alive(self):
		""" Check if we are in error state and dead/dying """
		return self._thread.isAlive()
	
	def error(self):
		""" Return the error message that killed us """
		if self._error_event.isSet():
			return self._error.__str__()
		else:
			return None
	
	def _validate_config(self):
		""" TODO: Sanity checks of the final config """
		allowed_globals = "watch_plugins,watch_config".split(',')
		for (key, val) in self._config['global'].iteritems():
			if not key in allowed_globals:
				if not self._thread.isAlive():
					raise iObserverError(self, "Illegal option '%s' in configuration." % key)
				else:
					iObserverError(self, "Illegal option '%s' in configuration." % key)
			elif not isinstance(val, str) and not isinstance(val, int):
				if not self._thread.isAlive():
					raise iObserverError(self, "Illegal option value '%s' for '%s' in configuration." % (val, key))
				else:
					iObserverError(self, "Illegal option value '%s' for '%s' in configuration." % (val, key))
	
	def _merge_config(self, config):
		""" For now, merge new the global section
		with the defaul one and add the watches one.
		Maybe do some deep copying later if
		more complex config is desired? """
		if config.has_key('global'):
			self._config['global'].update(config['global'])
		if config.has_key('watches'):
			self._config['watches'] = dict([ (os.path.realpath(watch), val) for (watch, val) in config['watches'].items()])
	
	def _read_config_file(self, file):
		""" Read... well... the config file:) """
		config = None
		try:
			config = ConfigObj(file, file_error=True)
		except IOError, data:
			raise iObserverError(self, "Error reading configuration file: %s" % data)
		except ConfigObjError, data:
			raise iObserverError(self, "Error parsing confiuration file: %s" % data)
		
		return config.dict()
	
	def _is_true(self, value):
		""" Check if value looks like something True.
		Used because configobj does not make value conversions."""
		if isinstance(value, bool) or isinstance(value, int):
			return value
		elif isinstance(value, str):
			if value == '1' or value.lower() == 'yes' or value.lower() == 'true':
				return True
			
			return False
	
	def _configure(self, config):
		""" Set default config values and read any config
		from a dict or a file"""
		self._config = {
			'global':{
				'watch_config': False,
				'watch_plugins': False,
			},
			'watches':{
			
			},
		}
		if config and isinstance(config, dict):
			self._merge_config(config)
			self._config_path = None
		elif config:
			self._config_path = os.path.realpath(config)
			try:
				from_file = self._read_config_file(config)
				self._merge_config(from_file)
			except iObserverError, data:
				if not self._thread.isAlive():
					raise
		
		self._validate_config()
	
	def _obey_global_option(self, option):
		""" Do what needs to be done for each
		global option """
		if option == 'watch_config':
			if self._is_true(self._config['global'][option]):
				# Turn on
				if self._config_path:
					self._config_watch = iPollWatch(
						observer=self,
						available_plugins = {'config_watch': self},
						config = {
							self._config_path: {
								'plugins': 'config_watch'
							}
						}
					)
					self._config_watch.start()
			else:
				# Turn off
				if self._config_watch and self._config_watch.is_alive():
					print "Stopping config watch."
					self._config_watch.stop()
					self._config_watch = None
		
		elif option == 'watch_plugins':
			if self._is_true(self._config['global']['watch_plugins']):
				# Turn on
				self._plugins_watch = iWatch(
					observer=self,
					available_plugins = {'plugins_watch': self},
					config = {
						plugins.__path__[0]: {
							'plugins': 'plugins_watch'
						}
					}
				)
				self._plugins_watch.start()
			else:
				# Turn off
				if self._plugins_watch and self._plugins_watch.is_alive():
					self._plugins_watch.stop()
					self._plugins_watch = None
		else:
			iObserverError(self, "_obey_global_option called with incorrect option '%s'" % option)
	
	def _update_config(self):
		""" Update the config when a change is detected. """
		old_config = copy.deepcopy(self._config)
		self._configure(self._config_path)
		
		# See what's changed and what needs to be done:
		for (option, value) in old_config['global'].iteritems():
			if self._is_true(value) != self._is_true(self._config['global'][option]):
				self._obey_global_option(option)
		
		# Stop watches that were removed from config file
		# and update the ones that are still running
		for watch in self._watches.keys():
			if not watch in self._config['watches'].keys():
				self._watches[watch].stop()
				self._watches.pop(watch)
			else:
				self._watches[watch].update_config(
					available_plugins=self._plugins,
					config={watch: self._config['watches'][watch]}
				)
		
		# Start any new watches
		for watch in self._config['watches'].keys():
			if not watch in self._watches.keys():
				self._watches[watch] = iWatch(
					observer=self,
					available_plugins=self._plugins,
					config={watch: self._config['watches'][watch]}
				)
				self._watches[watch].start()
	
	def _load_plugins(self):
		# Get a list of all plugins... (ignoring any names starting with _)
		# TODO use a more readable expression like:
		# [d for d in ... if ...]
		#self._plugins = dict(map(lambda x: (os.path.basename(x)[:-3], None), glob(plugins.__path__[0] + '/[!_]*.py')))
		
		self._plugins = dict([ (os.path.basename(x)[:-3], None)
						for x in glob(plugins.__path__[0] + '/[!_]*.py') ])
		
		# and load any that are not yet loaded.
		# Don't attempt to reload existing ones even if they might have changed,
		# because I don't know what the consequences be:)
		try: 
			for plugin in self._plugins.keys():
				if not sys.modules.has_key('iobserver.plugins.'+plugin):
					# Load new plugin
					imp.acquire_lock()
					
					found = imp.find_module(plugin, plugins.__path__)
					self._plugins[plugin] = imp.load_module('iobserver.plugins.'+plugin, found[0], found[1], found[2])
					
					imp.release_lock()
				
				else:
					# Reload plugin
					reload(sys.modules['iobserver.plugins.'+plugin])
		except Exception, data:
			if imp.lock_held(): imp.release_lock()
			if self._thread.isAlive():
				iObserverError(self, "Could not load plugin(s): %s" % data)
			else:
				raise iObserverError(None, "Could not load plugin(s): %s" % data)
	
	def process_event(self, event):
		""" Having this method makes us a valid plugin:)
		We use us as a plugin to handle both configuration
		and plugin changes. """
		
		# Not running in the main thread of the instance
		
		if not event.event_name in "IN_CREATE,IN_DELETE,IN_DELETE_SELF,IN_MODIFY,IN_MOVE_SELF,IN_MOVED_FROM,IN_MOVED_TO".split(','):
			return
		
		if event.path == self._config_path:
			# Event is about the configuration file
			self._config_changed_event.set()
		else:
			# Event is about plugins directory
			
			# NOTE: This is now handled by the watch itself!
			## If we are moved/deleted, stop watching...
			#if event.event_name == 'IN_MOVE_SELF' or event.event_name == 'IN_DELETE_SELF':
				#watch.stop()
				#return
			
			if event.name.endswith('.pyc'):
				# Ignore changes in the compiled modules, as they occur when
				# a changed module is loaded and will cause unneccessery reload.
				return
			if event.name.startswith('.'):
				# Ignore hidden files
				return
			self._plugins_changed_event.set()
	
	def start(self):
		""" Start our new thread. """
		try:
			self._thread.start()
		except:
			iObserverError(self, "Could not start observer thread.")
	
	def run(self):
		""" The target passed to our thread to execute. """
		self._watches = {}
		
		# Set up our special config/plugin watches
		self._obey_global_option('watch_config')
		
		self._obey_global_option('watch_plugins')
		
		# Set up all other configured watches
		for watch in self._config['watches'].keys():
			self._watches[watch] = iWatch(
				observer=self,
				available_plugins=self._plugins,
				config={watch: self._config['watches'][watch]}
			)
		
		# Fire in the hole!
		for watch in self._watches.values():
			watch.start()
		
		# This thread now waits for various events:
		#  - terminate event
		#  - error
		#  - configuration changed event
		while True:
			self._terminate_event.wait(1)
			if self._terminate_event.isSet():
				# Exiting
				break
			if self._error_event.isSet():
				break
			if self._plugins_changed_event.isSet():
				self._plugins_changed_event.clear()
				self._load_plugins()
			if self._config_changed_event.isSet():
				self._config_changed_event.clear()
				self._update_config()
				
		
		for watch in self._watches.values():
			if watch.is_alive:
				watch.stop()
		
		if self._config_watch: self._config_watch.stop()
		if self._plugins_watch: self._plugins_watch.stop()
		
	def stop(self):
		""" Called from application thread. """
		self._terminate_event.set()