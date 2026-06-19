#!/usr/bin/env python3

import threading
import time
import logging
from collections import deque
from datetime import datetime

import matplotlib.pyplot as plt

from air_pollution_source import air_pollution_source
from weather_source import weather_source
from stream import stream


def bufferless_fuzzy_merge_join(dominant_stream, nondominant_stream, oStream, stats):
    """Bufferless fuzzy merge join that records dropped dominant tuples."""
    pending_nondominant = deque()

    while True:
        while True:
            nondominant_candidate = nondominant_stream.inspect()
            if nondominant_candidate is None:
                break
            pending_nondominant.append(nondominant_stream.get())

        dominant_candidate = dominant_stream.inspect()
        if dominant_candidate is None:
            time.sleep(0.05)
            continue

        dominant_item = dominant_stream.get()
        dominant_ts, dominant_value = dominant_item

        if len(pending_nondominant) == 0:
            stats["dropped_count"] += 1
            stats["dropped_over_time"].append((dominant_ts, stats["dropped_count"]))
            logging.info("dropped dominant tuple %s because no nondominant partner was available", dominant_item)
            continue

        partner_index = None
        for idx in range(len(pending_nondominant) - 1, -1, -1):
            if pending_nondominant[idx][0] <= dominant_ts:
                partner_index = idx
                break

        if partner_index is None:
            stats["dropped_count"] += 1
            stats["dropped_over_time"].append((dominant_ts, stats["dropped_count"]))
            logging.info("dropped dominant tuple %s because no earlier nondominant tuple exists", dominant_item)
            continue

        nondominant_item = pending_nondominant[partner_index]
        del pending_nondominant[partner_index]

        result = (dominant_ts, dominant_value, nondominant_item[0], nondominant_item[1])
        oStream.put_force(result)
        logging.info("joined tuple: %s", result)


def sink(iStream, max_results, results):
    while len(results) < max_results:
        result = iStream.get()
        results.append(result)
        ts, pollution_val, weather_ts, weather_val = result
        logging.info("[SINK] #%d: pollution=%.2f @ %.3f, temperature=%.2f @ %.3f",
                     len(results), pollution_val, ts, weather_val, weather_ts)


def plot_results(results, dropped_over_time):
    if not results:
        logging.warning("No join results to plot.")
        return

    t0 = results[0][0]
    times = [r[0] - t0 for r in results]
    pollution_values = [r[1] for r in results]
    temperature_values = [r[3] for r in results]

    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(10, 8))

    ax1.plot(times, pollution_values, marker='o', color='tab:green', label='Air pollution')
    ax1.set_ylabel('Air pollution value')
    ax1.set_title('Bufferless Fuzzy Merge Join — Joined air pollution and temperature')
    ax1.grid(True, linestyle='--', alpha=0.4)

    ax1b = ax1.twinx()
    ax1b.plot(times, temperature_values, marker='s', color='tab:orange', label='Temperature')
    ax1b.set_ylabel('Temperature (°C)', color='tab:orange')
    ax1b.tick_params(axis='y', labelcolor='tab:orange')

    if dropped_over_time:
        d0 = dropped_over_time[0][0]
        drop_times = [d[0] - d0 for d in dropped_over_time]
        drop_counts = [d[1] for d in dropped_over_time]
        ax2.step(drop_times, drop_counts, where='post', color='tab:red')
    ax2.set_xlabel('Time since first dropped tuple (s)')
    ax2.set_ylabel('Cumulative dropped dominant tuples')
    ax2.set_title('Bufferless Fuzzy Merge Join — Dropped tuples over time')
    ax2.grid(True, linestyle='--', alpha=0.4)

    fig.tight_layout()
    plot_path = 'bufferless_fuzzy_merge_join_plot.png'
    fig.savefig(plot_path)
    logging.info('Plot saved to %s', plot_path)

    try:
        plt.show()
    except Exception:
        logging.info('Plot display unavailable; open %s manually instead.', plot_path)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='[%(levelname)s] (%(threadName)-12s) %(message)s')

    MAX_RESULTS = 30

    stream_pollution = stream('Air Pollution Stream', 10)
    stream_weather = stream('Weather Stream', 10)
    stream_result = stream('Join Stream', 10)

    results = []
    stats = {'dropped_count': 0, 'dropped_over_time': []}

    t_pollution = threading.Thread(name='air_pollution', target=air_pollution_source, args=(stream_pollution,), daemon=True)
    t_weather = threading.Thread(name='weather', target=weather_source, args=(stream_weather,), daemon=True)
    t_join = threading.Thread(name='bfmj_operator', target=bufferless_fuzzy_merge_join,
                              args=(stream_pollution, stream_weather, stream_result, stats), daemon=True)

    t_pollution.start()
    t_weather.start()
    t_join.start()

    sink(stream_result, MAX_RESULTS, results)
    plot_results(results, stats['dropped_over_time'])
