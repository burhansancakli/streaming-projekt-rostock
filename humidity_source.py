from datetime import datetime
import logging
import random
import time

from stream import stream


# humidity data — faster stream, recessive in the Fuzzy Merge Join
def humidity_source(oStream: stream):
    humidity = 50.0  # percent, starting point
    while True:
        # random walk, clamped to a realistic 0-100% range
        humidity += random.uniform(-2.0, 2.0)
        humidity = max(0.0, min(100.0, humidity))
        ts = datetime.timestamp(datetime.now())
        t = (ts, humidity)
        oStream.put_force(t)
        # logging.debug("produced %.2f%% @ %f" % (humidity, ts))
        time.sleep(random.random() * 0.2 + 0.4)  # ~0.4-0.6s/tuple, faster than weather_source
