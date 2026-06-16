# ex for tuple producer - cumulative sensor energy consumption
from datetime import datetime
import logging
import random
import time

from stream import stream

# pollution sensor energy consumption per interval (watt-hours)
def sensor_energy_source(oStream: stream):
	power_draw_range = (2.0, 10.0)  # watts
	interval_range = (5.0, 20.0)  # seconds between readings
	
	while True:
		# Simulate varying power draw and variable read frequency
		power_draw = random.uniform(*power_draw_range)
		interval = random.uniform(*interval_range)
		# Energy consumed in this interval (interval seconds)
		energy_consumed = power_draw * interval / 3600  # Convert watts to wh
		
		ts = datetime.timestamp(datetime.now())
		t = (ts, energy_consumed)
		oStream.put_force(t)
		#logging.debug("produced %.4f wh @ %f" % (energy_consumed, ts))
		time.sleep(interval) # mimic variable duty cycle
