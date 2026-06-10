# ex for tuple producer
from datetime import datetime
import logging
import random
import time

from stream import stream

def luftverschmutzung_quelle(oStream: stream):
	pn25 = range(10,80)
	while True:
		num = random.choice(pn25)
		ts = datetime.timestamp(datetime.now());
		t = (ts, num)
		oStream.put_force(t)
		#logging.debug("produced %d @ %f" % (num, ts))
		time.sleep(random.random()/10+0.1) # mimic heavy duty