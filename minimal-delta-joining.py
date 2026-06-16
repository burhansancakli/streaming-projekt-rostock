#!/usr/bin/env python3

import threading
import time
from datetime import datetime
import random
import logging

import time
import matplotlib.pyplot as plt
from datetime import datetime
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

from queue import Empty



def minimal_delta_joining(stream1, stream2, window_size=10):

    joined_list = []
    t1 = time.time()
    dominant = 1

    item1 = []
    item2 = []

    # -----------------------------
    # LIVE PLOT INIT
    # -----------------------------
    plt.ion()
    fig, ax = plt.subplots()

    scatter1 = ax.scatter([], [], label="Data1")
    scatter2 = ax.scatter([], [], label="Data2")

    ax.set_xlabel("Time (timestamp)")
    ax.set_ylabel("Value")
    ax.legend()
    ax.grid(True)

    while True:

        # -----------------------------
        # STREAM INGESTION
        # -----------------------------
        while stream1.inspect() is not None:
            item1.append(stream1.get())

        while stream2.inspect() is not None:
            item2.append(stream2.get())

        # -----------------------------
        # JOIN LOGIC
        # -----------------------------
        while True:

            if dominant == 1 and len(item1) > 0:
                norm = item1[0][0]
                changed = False

                for i in range(len(item2)):
                    if item2[i][0] >= norm:

                        if i == 0:
                            joined_list.append((norm, item1[0][1], item2[i][1]))
                            item1.pop(0)
                            item2 = item2[1:]
                        else:
                            delta1 = norm - item2[i-1][0]
                            delta2 = item2[i][0] - norm

                            if delta1 <= delta2:
                                joined_list.append((norm, item1[0][1], item2[i-1][1]))
                                item2 = item2[i:]
                                dominant = 2
                            else:
                                joined_list.append((norm, item1[0][1], item2[i][1]))
                                item1.pop(0)
                                item2 = item2[i:]

                        changed = True
                        break

                if not changed:
                    break

            elif dominant == 2 and len(item2) > 0:
                norm = item2[0][0]
                changed = False

                for i in range(len(item1)):
                    if item1[i][0] >= norm:

                        if i == 0:
                            joined_list.append((norm, item1[i][1], item2[0][1]))
                            item1 = item1[1:]
                            item2.pop(0)
                        else:
                            delta1 = norm - item1[i-1][0]
                            delta2 = item1[i][0] - norm

                            if delta1 <= delta2:
                                joined_list.append((norm, item1[i-1][1], item2[0][1]))
                                item1 = item1[i:]
                                dominant = 1
                            else:
                                joined_list.append((norm, item1[i][1], item2[0][1]))
                                item1 = item1[i:]
                                item2.pop(0)

                        changed = True
                        break

                if not changed:
                    break

            else:
                break

        # -----------------------------
        # LIVE PLOT UPDATE (SCATTER ONLY)
        # -----------------------------
        if len(joined_list) > 0:

            t0 = joined_list[0][0]

            times = [x[0] - t0 for x in joined_list]  # WICHTIG: relativ!
            data1 = [x[1] for x in joined_list]
            data2 = [x[2] for x in joined_list]

            offsets1 = list(zip(times, data1))
            offsets2 = list(zip(times, data2))

            scatter1.set_offsets(offsets1)
            scatter2.set_offsets(offsets2)

            # WICHTIG: explizite Limits statt relim()
            ax.set_xlim(min(times), max(times) if len(times) > 1 else 1)
            
            all_y = data1 + data2
            ax.set_ylim(min(all_y), max(all_y) if len(all_y) > 1 else 1)

            fig.canvas.draw()
            fig.canvas.flush_events()
        # -----------------------------
        # WINDOW RESETg
        # -----------------------------
        if time.time() - t1 > window_size:

            if len(joined_list) > 0:
                logging.info("joined tuples: %s", joined_list)

                for item in joined_list:
                    logging.debug("minimal delta joining: %s", item)

                joined_list = []

            t1 = time.time()

        time.sleep(0.01)
logging.basicConfig(level=logging.DEBUG, format='[%(levelname)s] (%(threadName)-10s) %(message)s')

# define two streams with different sizes
luftverschmutzung_stream = stream("Luftverschmutzung Stream", 10)
wetter_stream = stream("Wetter Stream", 10)
# the output stream for the join results
merge_stream = stream("Join Stream", 10)

# create threads for three operators 2 sources and one join operator and one sink
luftverschmutzung_thread = threading.Thread(name='luftverschmutzung', target=luftverschmutzung_quelle, args=(luftverschmutzung_stream,))
wetter_thread = threading.Thread(name='wetter', target=wetter_quelle, args=(wetter_stream,))
join_thread = threading.Thread(name='join', target=minimal_delta_joining, args=(wetter_stream, luftverschmutzung_stream, 50))
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
