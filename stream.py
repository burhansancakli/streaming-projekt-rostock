import threading
import logging


# simple stream with window size
class stream:
	def __init__(self, name, winsize):
		self._name = name
		self._winsize = winsize
		self._stream = [None]*winsize # array with winsize None elements
		self._cnt = self._rpos = self._wpos = 0
		self._mutex = threading.Condition()
	def __len__(self):
		return len(self._stream) # or self._winsize
	def __str__(self):
		return "stream(name=%s, q=%s, cnt=%d, rpos=%d, wpos=%d)" % (self._name, str(self._stream), self._cnt, self._rpos, self._wpos)
	def _isfull(self):
		return self._rpos == self._wpos and self._cnt == self._winsize
	def _isempty(self):
		return self._rpos == self._wpos and self._cnt == 0
	def _enqueue(self, t):
		self._cnt += 1
		self._stream[self._wpos] = t
		if self._wpos + 1 == self._winsize:
			self._wpos = 0
		else:
			self._wpos += 1
	def _dequeue(self):
		t = self._stream[self._rpos]
		self._stream[self._rpos] = None # frees the object
		if self._rpos + 1 == self._winsize:
			self._rpos = 0
		else:
			self._rpos += 1
		self._cnt -= 1
		return t

	# public methods, the stuff above should only be called withing
	# the guarded commands acquire()/release() to be thread-safe
	#
	# put a something on the outgoing stream
	def put(self, t):
		self._mutex.acquire()
		if self._isfull():
			logging.debug("stream before blocking on full = %s" % str(self))
			self._mutex.wait() # wait for tuples to be dequeued
			logging.debug("put released after full stream")
		self._enqueue(t)
		self._mutex.notify()
		self._mutex.release()
	
	# delete the last thing if the stream is full, put the new thing
	def put_force(self, t):
		self._mutex.acquire()
		if self._isfull():
			self._dequeue() # delete the last thing
			#logging.debug("stream after force delete on full = %s" % str(self))
		self._enqueue(t)
		self._mutex.notify()
		self._mutex.release()

	# get a something from an input stream
	def get(self):
		self._mutex.acquire()
		if self._isempty():
			#logging.debug("get blocked by empty stream")
			self._mutex.wait() # wait for tuples to be enqueued
			#logging.debug("stream after release on empty = %s" % str(self))
		t = self._dequeue()
		self._mutex.notify()
		self._mutex.release()
		return t
	
	# get a something from an input stream, non blocking and non consuming
	def inspect(self):
		self._mutex.acquire()
		if self._isempty():
			#logging.debug("inspect empty stream")
			t = None
		else:
			t = self._stream[self._rpos]
		self._mutex.release()
		return t
