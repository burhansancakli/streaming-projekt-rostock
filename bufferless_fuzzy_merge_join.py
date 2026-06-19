#!/usr/bin/env python3

import threading
import time
import random
import logging
from collections import deque

from air_pollution_source import air_pollution_source
from stream import stream
from weather_source import weather_source

#######################

# ex for tuple consumer
def sink(iStream):
	while True:
		something = iStream.get()
		logging.debug("consumed %s" % str(something))
		time.sleep(random.random()*10) # mimic heavy duty

# weather data timestamp 1500
# air pollution timestamp 1501

def bufferless_fuzzy_merge_join(dominant_stream, nondominant_stream, out_stream):
	"""
	Bufferless fuzzy merge join based on increasing timestamps.
	The dominant stream is the faster stream. The nondominant stream is the slower
	stream. Each dominant tuple is joined with the most recent nondominant tuple
	that has a timestamp <= dominant timestamp. If no partner exists, the dominant
	tuple is dropped.

	Each tuple may only be used once in the join.
	"""
	pending_nondominant = deque()

	while True:
		# Drain any available nondominant tuples into the pending buffer.
		while True:
			nondominant_candidate = nondominant_stream.inspect()
			if nondominant_candidate is None:
				break
			pending_nondominant.append(nondominant_stream.get())

		# If no dominant tuple is ready, wait until one arrives.
		dominant_candidate = dominant_stream.inspect()
		if dominant_candidate is None:
			time.sleep(0.05)
			continue

		dominant_item = dominant_stream.get()
		dominant_ts = dominant_item[0]

		if len(pending_nondominant) == 0:
			logging.info("dropped dominant tuple %s because no nondominant partner was available", dominant_item)
			continue

		# Find the most recent nondominant tuple with ts <= dominant_ts.
		partner_index = None
		for idx in range(len(pending_nondominant) - 1, -1, -1):
			if pending_nondominant[idx][0] <= dominant_ts:
				partner_index = idx
				break

		if partner_index is None:
			logging.info("dropped dominant tuple %s because no earlier nondominant tuple exists", dominant_item)
			continue

		nondominant_item = pending_nondominant[partner_index]
		del pending_nondominant[partner_index]

		joined_item = (
			dominant_item[0],
			dominant_item[1],
			nondominant_item[0],
			nondominant_item[1],
		)
		logging.info("joined tuple: %s", joined_item)
		out_stream.put_force(joined_item)


logging.basicConfig(level=logging.DEBUG, format='[%(levelname)s] (%(threadName)-10s) %(message)s')

# define two streams with different sizes
air_pollution_stream = stream("Air Pollution Stream", 10)
weather_stream = stream("Weather Stream", 10)
# the output stream for the join results
merge_stream = stream("Join Stream", 10)

# create threads for three operators 2 sources and one join operator and one sink
air_pollution_thread = threading.Thread(name='air_pollution', target=air_pollution_source, args=(air_pollution_stream,))
weather_thread = threading.Thread(name='weather', target=weather_source, args=(weather_stream,))
join_thread = threading.Thread(name='join', target=bufferless_fuzzy_merge_join, args=(air_pollution_stream, weather_stream, merge_stream,))
sink_thread = threading.Thread(name='sink', target=sink, args=(merge_stream,))

air_pollution_thread.start()
weather_thread.start()
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
