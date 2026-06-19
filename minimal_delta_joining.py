#!/usr/bin/env python3

import threading
import time
import argparse
from datetime import datetime
import random
import logging

# import matplotlib
# matplotlib.use('Agg')
# import matplotlib.pyplot as plt
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

from queue import Empty



def minimal_delta_joining(stream1, stream2, window_size=10):

    joined_list = []
    t1 = time.time()
    dominant = 1

    item1 = []
    item2 = []

    # # # -------- PLOT DISABLED --------
    # # plt.ion()
    # # fig, ax = plt.subplots()
    # # scatter1 = ax.scatter([], [], label="Data1")
    # # scatter2 = ax.scatter([], [], label="Data2")
    # # ax.set_xlabel("Time (timestamp)")
    # # ax.set_ylabel("Value")
    # # ax.legend()
    # # ax.grid(True)
    # # plot_path = 'minimal_delta_joining_plot.png'

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

        # # # -------- PLOT DISABLED --------
        # # if len(joined_list) > 0:
        # #     t0 = joined_list[0][0]
        # #     times = [x[0] - t0 for x in joined_list]
        # #     data1 = [x[1] for x in joined_list]
        # #     data2 = [x[2] for x in joined_list]
        # #     offsets1 = list(zip(times, data1))
        # #     offsets2 = list(zip(times, data2))
        # #     scatter1.set_offsets(offsets1)
        # #     scatter2.set_offsets(offsets2)
        # #     ax.set_xlim(min(times), max(times) if len(times) > 1 else 1)
        # #     all_y = data1 + data2
        # #     ax.set_ylim(min(all_y), max(all_y) if len(all_y) > 1 else 1)
        # #     fig.canvas.draw()
        # #     fig.canvas.flush_events()
        # # # -------- WINDOW RESET & PLOT SAVE DISABLED --------
        # # if time.time() - t1 > window_size:
        # #     if len(joined_list) > 0:
        # #         logging.info("joined tuples: %s", joined_list)
        # #         for item in joined_list:
        # #             logging.debug("minimal delta joining: %s", item)
        # #         try:
        # #             fig.savefig(plot_path)
        # #             logging.info('Plot saved to %s', plot_path)
        # #         except Exception as err:
        # #             logging.warning('Failed to save plot: %s', err)
        # #         joined_list = []
        # #     t1 = time.time()

        time.sleep(0.01)
logging.basicConfig(level=logging.DEBUG, format='[%(levelname)s] (%(threadName)-10s) %(message)s')

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Minimal Delta Join operator')
    parser.add_argument('--source1', type=str, default='weather',
                        choices=['air_pollution', 'weather', 'humidity', 'sensor_energy'],
                        help='First stream source (default: weather)')
    parser.add_argument('--source2', type=str, default='air_pollution',
                        choices=['air_pollution', 'weather', 'humidity', 'sensor_energy'],
                        help='Second stream source (default: air_pollution)')
    parser.add_argument('--window-size', type=int, default=50,
                        help='Window size in seconds (default: 50)')
    args = parser.parse_args()

    sources = {
        'air_pollution': ('Air Pollution', air_pollution_source),
        'weather': ('Weather', weather_source),
        'humidity': ('Humidity', humidity_source),
        'sensor_energy': ('Sensor Energy', sensor_energy_source),
    }

    source1_name, source1_func = sources[args.source1]
    source2_name, source2_func = sources[args.source2]

    logging.warning("=" * 60)
    logging.warning(f"  MINIMAL DELTA JOIN — {source1_name} x {source2_name}")
    logging.warning(f"  Stream1: {source1_name}")
    logging.warning(f"  Stream2: {source2_name}")
    logging.warning(f"  Window: {args.window_size}s")
    logging.warning("=" * 60)

    stream1 = stream(f"{source1_name} Stream", 10)
    stream2 = stream(f"{source2_name} Stream", 10)

    source1_thread = threading.Thread(name='source1', target=source1_func, args=(stream1,))
    source2_thread = threading.Thread(name='source2', target=source2_func, args=(stream2,))
    join_thread = threading.Thread(name='join', target=minimal_delta_joining, args=(stream1, stream2, args.window_size))
    sink_thread = threading.Thread(name='sink', target=sink, args=(None,))

    source1_thread.start()
    source2_thread.start()
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
