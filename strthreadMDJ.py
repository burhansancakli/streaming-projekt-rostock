#!/usr/bin/env python3

import threading
import time
from datetime import datetime
import random
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

	# get a something from an input stream
	def get(self):
		self._mutex.acquire()
		if self._isempty():
			logging.debug("get blocked by empty stream")
			self._mutex.wait() # wait for tuples to be enqueued
			logging.debug("stream after release on empty = %s" % str(self))
		t = self._dequeue()
		self._mutex.notify()
		self._mutex.release()
		return t
	
	# get a something from an input stream, non blocking and non consuming
	def inspect(self):
		self._mutex.acquire()
		if self._isempty():
			logging.debug("inspect empty stream")
			t = None
		else:
			t = self._stream[self._rpos]
		self._mutex.release()
		return t

#######################

# ex for tuple producer
def fountain(oStream):
	nums = range(20)
	while True:
		num = random.choice(nums)
		ts = datetime.timestamp(datetime.now());
		t = (ts, num)
		access = oStream._isfull()
		oStream.put(t)
		logging.debug("produced %d @ %f" % (num, ts))
		if not access:
			minimal_delta_joining("fountain", t)
		time.sleep(random.random()) # mimic heavy duty

# ex for tuple consumer
def sink(iStream):
	while True:
		something = iStream.get()	
  
		logging.debug("consumed %s" % str(something))
		if something is not None:
			minimal_delta_joining("sink", something)
		time.sleep(random.random()) # mimic heavy duty

# ex for tuple filter
def filter(iStream, oStream):
	while True:
		access1 = iStream._isempty()
		(ts, num1) = iStream.get()
		logging.debug("filtered %d @ %f" % (num1, ts))
		access2 = oStream._isfull()
		oStream.put((ts, num1))
		if not access2 and not access1:
			minimal_delta_joining("filter", (ts, (num1, num1)))
		if not access1:
			minimal_delta_joining("filter", (ts, (num1, None)))
		if not access2:
			minimal_delta_joining("filter", (ts, (None, num1)))
		time.sleep(random.random()) # mimic heavy duty

# ex for tuple min/max filter
def filterMinMax(iStream, oStream):
	_min = float('Inf')
	_max = -float('Inf')
	while True:
		# consume one tuple
		(ts, num) = iStream.get()
		if num < _min:
			_min = num
		if num > _max:
			_max = num
		logging.debug("(min, max) = (%f, %f)" % (_min, _max))
		oStream.put((_min,_max))
		time.sleep(random.random()) # mimic heavy duty




def continues(typ, t = None):
	for i in range(1,len( dic[typ])):
		dic[typ][i-1] = dic[typ][i]
	dic[typ][-1] = t
def insert_new(typ, t):
	dici = dic[typ]
	if None in dici:
		dici[dici.index(None)] = t
	else:
		continues(typ, t)

def min_delta(data1, data2, data3, typess):
	time_temp = data1[0]

	t1 = abs(time_temp - data2[0])
	t2 = abs(time_temp - data3[0])
	if t1 < t2:
			return data2
	else:
			return data3

def minimal_delta_joining(typess, data1, rec = False):
	global dom

	if not rec:
		insert_new(typess, data1)
	if dom == typess:
		if all(dic[x][1] is not None for x in ["fountain", "filter", "sink"] if x != typess):
			if dom != "fountain":
				fount = min_delta(dic[typess][0], dic["fountain"][0], dic["fountain"][1], typess)
			else:
				fount = dic[typess][0]
			if dom != "filter":
				filt = min_delta(dic[typess][0], dic["filter"][0], dic["filter"][1], typess)
			else:
				filt = dic[typess][0]
			if dom != "sink":
				sink = min_delta(dic[typess][0], dic["sink"][0], dic["sink"][1], typess)
			else:
				sink = dic[typess][0]
			res = min(
				(fount, filt, sink),
				key=lambda x: x[0]
			)
			print("minimal delta joining: %s with result: %s" % (str((fount, filt, sink)), str(res)))
			result.append((dic[dom][0],(fount[1], filt[1], sink[1])))
			if res == fount:
				continues("fountain")
				dom = "fountain"
				
			elif res == filt:
				continues("filter")
				dom = "filter"
			else:
				continues("sink")
				dom = "sink"
		
			print("joined data: %s" % str((fount, filt, sink)))
			if dic[dom][0] is not None:
				minimal_delta_joining(dom, dic[dom][0], True)
			if len(result) > 50:
				print("final result: %s" % str(result))
				result.clear()
# vim: ts=3 sw=3 sts=3 noet
result = []
dic = {"fountain": [None, None, None,None,None,None,None,None,None,None],
       "filter": [None, None, None,None,None,None,None,None,None,None],
       "sink": [None, None, None,None,None,None,None,None,None,None]}
dom = "fountain"
logging.basicConfig(level=logging.DEBUG, format='[%(levelname)s] (%(threadName)-10s) %(message)s')

# define two streams with different sizes
st1 = stream("Stream 1", 10)
st2 = stream("Stream 2", 5)

# create threads for three operators one source (fountain), one filter, and one destination (sink)
src = threading.Thread(name='fountain', target=fountain, args=(st1,))
flt = threading.Thread(name='filter', target=filter, args=(st1,st2))
dst = threading.Thread(name='sink', target=sink, args=(st2,))

flt.start()
dst.start()
src.start()

# vim: ts=3 sw=3 sts=3 noet