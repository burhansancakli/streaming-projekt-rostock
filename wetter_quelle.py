# ex for tuple producer
from datetime import datetime
import logging
import random
import time

from stream import stream

def wetter_quelle(oStream: stream):
	temperatur = 25
	while True:
		temperatur += random.random()
		ts = datetime.timestamp(datetime.now());
		t = (ts, temperatur)
		oStream.put_force(t)
		#logging.debug("produced %d @ %f" % (temperatur, ts))
		time.sleep((random.random()+1.0)) # mimic heavy duty