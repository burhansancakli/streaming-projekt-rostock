#!/usr/bin/env python3

import threading
import time
import random
import logging
import argparse
from collections import deque

from air_pollution_source import air_pollution_source
from weather_source import weather_source
from humidity_source import humidity_source
from sensor_energy_source import sensor_energy_source
from stream import stream

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

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Bufferless Fuzzy Merge Join operator')
    parser.add_argument('--dominant-source', type=str, default='air_pollution',
                        choices=['air_pollution', 'weather', 'humidity', 'sensor_energy'],
                        help='Dominant stream (faster) source (default: air_pollution)')
    parser.add_argument('--nondominant-source', type=str, default='weather',
                        choices=['air_pollution', 'weather', 'humidity', 'sensor_energy'],
                        help='Nondominant stream (slower) source (default: weather)')
    args = parser.parse_args()

    sources = {
        'air_pollution': ('Air Pollution', air_pollution_source),
        'weather': ('Weather', weather_source),
        'humidity': ('Humidity', humidity_source),
        'sensor_energy': ('Sensor Energy', sensor_energy_source),
    }

    dominant_name, dominant_func = sources[args.dominant_source]
    nondominant_name, nondominant_func = sources[args.nondominant_source]

    logging.warning("=" * 60)
    logging.warning(f"  BUFFERLESS FUZZY MERGE JOIN — {dominant_name} x {nondominant_name}")
    logging.warning(f"  Dominant: {dominant_name} (faster)")
    logging.warning(f"  Nondominant: {nondominant_name} (slower)")
    logging.warning("=" * 60)

    dominant_stream = stream(f"{dominant_name} Stream", 10)
    nondominant_stream = stream(f"{nondominant_name} Stream", 10)
    merge_stream = stream("Join Stream", 10)

    dominant_thread = threading.Thread(name='dominant', target=dominant_func, args=(dominant_stream,))
    nondominant_thread = threading.Thread(name='nondominant', target=nondominant_func, args=(nondominant_stream,))
    join_thread = threading.Thread(name='join', target=bufferless_fuzzy_merge_join, args=(dominant_stream, nondominant_stream, merge_stream,))
    sink_thread = threading.Thread(name='sink', target=sink, args=(merge_stream,))

    dominant_thread.start()
    nondominant_thread.start()
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
