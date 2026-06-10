#!/usr/bin/env python3

import threading
import time
from datetime import datetime
import random
import logging

from luftverschmutzung_quelle import luftverschmutzung_quelle
from stream import stream
from wetter_quelle import wetter_quelle

#######################

# ex for tuple consumer
def sink(iStream):
	while True:
		something = iStream.get()
		logging.debug("consumed %s" % str(something))
		time.sleep(random.random()*10) # mimic heavy duty

def bufferless_fuzzy_merge_join(dominant_stream, recessive_stream, out_stream):
	"""
	Bufferloser Fuzzy-Merge-Join based on increasing Timestamps.
	The dominant Stream delivers a tuple, and the recessive Stream delivers the
	next matching tuple with a Timestamp >= dominantTimestamp.
	"""
	while True:
		dominant_item = dominant_stream.get()
		#logging.debug("join dominant tuple %s", dominant_item)
		while True:
			recessive_candidate = recessive_stream.inspect()
			if recessive_candidate is None:
				time.sleep(0.05)
				continue
			if recessive_candidate[0] < dominant_item[0]:
				dropped = recessive_stream.get()
				#logging.debug("drop stale recessive tuple %s", dropped)
				continue
			recessive_item = recessive_stream.get()
			joined_item = (
				dominant_item[0],
				dominant_item[1],
				recessive_item[0],
				recessive_item[1],
			)
			logging.info("joined tuple: %s", joined_item)
			out_stream.put_force(joined_item)
			break


logging.basicConfig(level=logging.DEBUG, format='[%(levelname)s] (%(threadName)-10s) %(message)s')

# define two streams with different sizes
luftverschmutzung_stream = stream("Luftverschmutzung Stream", 10)
wetter_stream = stream("Wetter Stream", 10)
# the output stream for the join results
merge_stream = stream("Join Stream", 10)

# create threads for three operators 2 sources and one join operator and one sink
luftverschmutzung_thread = threading.Thread(name='luftverschmutzung', target=luftverschmutzung_quelle, args=(luftverschmutzung_stream,))
wetter_thread = threading.Thread(name='wetter', target=wetter_quelle, args=(wetter_stream,))
join_thread = threading.Thread(name='join', target=bufferless_fuzzy_merge_join, args=(wetter_stream, luftverschmutzung_stream, merge_stream,))
sink_thread = threading.Thread(name='sink', target=sink, args=(merge_stream,))

luftverschmutzung_thread.start()
wetter_thread.start()
join_thread.start()
sink_thread.start()


#import json
#
#
#kreise = []
#with open("kreise.json", "r", encoding="utf-8") as f:
#    data = json.load(f)
#    for item in data["features"]:
#        kreis = {**item["properties"],  **item["geometry"]}
#        kreise.append(kreis)
#    #import pandas as pd
#    #df = pd.json_normalize(data)
#    #print(df.head())
#    #print(df.columns)
#if len(kreise) == 0:
#    
#    print("No kreise found in kreise.json")
#    raise Exception("No kreise found in kreise.json")
#
#print("Kreise loaded: %d" % len(kreise))
#print(kreise[0])
# vim: ts=3 sw=3 sts=3 noet
