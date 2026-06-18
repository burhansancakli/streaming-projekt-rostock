#!/usr/bin/env python3

import threading
import time
import logging

import matplotlib.pyplot as plt

from weather_source import weather_source
from humidity_source import humidity_source
from stream import stream


# ─────────────────────────────────────────
# Same FMJ logic as fuzzy_merge_join.py, but the sink stops after
# `max_results` joins and produces two graphs:
#   1. Temperature & Humidity values over time, for every joined pair
#   2. Cumulative dropped-tuple count over time
# ─────────────────────────────────────────

def fuzzy_merge_join(iStream_S, iStream_T, oStream, stats):
    """Same join logic as fuzzy_merge_join.py, but also records dropped-tuple
    timestamps into the shared `stats` dict for plotting."""
    join_count = 0
    dropped_count = 0

    while True:
        tuple_S = iStream_S.get()
        ts_S = tuple_S[0]
        val_S = tuple_S[1]

        found_partner = False
        while not found_partner:
            tuple_T = iStream_T.get()
            ts_T = tuple_T[0]
            val_T = tuple_T[1]

            if ts_T < ts_S:
                dropped_count += 1
                stats["dropped_over_time"].append((ts_T, dropped_count))
            else:
                found_partner = True
                result_tuple = (ts_S, val_S, val_T)
                oStream.put_force(result_tuple)
                join_count += 1
                logging.info("JOIN #%d: Temp(ts=%.3f, val=%.2f) + Humidity(ts=%.3f, val=%.2f)"
                             % (join_count, ts_S, val_S, ts_T, val_T))


def sink(iStream, max_results, results):
    """Collects up to max_results joined tuples, then signals completion."""
    while len(results) < max_results:
        result = iStream.get()
        results.append(result)
        ts, val_temp, val_humidity = result
        logging.info("[SINK] #%d: temp=%.2f, humidity=%.2f", len(results), val_temp, val_humidity)


def plot_results(results, dropped_over_time):
    if not results:
        logging.warning("No results to plot.")
        return

    t0 = results[0][0]
    times = [r[0] - t0 for r in results]
    temps = [r[1] for r in results]
    humids = [r[2] for r in results]

    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(10, 8))

    # Plot 1: joined Temperature x Humidity values over time
    ax1.plot(times, temps, marker='o', color='tab:red', label='Temperature (deg C)')
    ax1.set_ylabel('Temperature (deg C)', color='tab:red')
    ax1.tick_params(axis='y', labelcolor='tab:red')
    ax1.set_title('FMJ — Joined Temperature x Humidity values over time')
    ax1.grid(True, linestyle='--', alpha=0.4)

    ax1b = ax1.twinx()
    ax1b.plot(times, humids, marker='s', color='tab:blue', label='Humidity (%)')
    ax1b.set_ylabel('Humidity (%)', color='tab:blue')
    ax1b.tick_params(axis='y', labelcolor='tab:blue')

    # Plot 2: cumulative dropped tuple count over time
    if dropped_over_time:
        d0 = dropped_over_time[0][0]
        drop_times = [d[0] - d0 for d in dropped_over_time]
        drop_counts = [d[1] for d in dropped_over_time]
        ax2.step(drop_times, drop_counts, where='post', color='tab:orange')
    ax2.set_xlabel('Time since first result (s)')
    ax2.set_ylabel('Cumulative dropped Humidity tuples')
    ax2.set_title('FMJ — Dropped tuple count over time')
    ax2.grid(True, linestyle='--', alpha=0.4)

    fig.tight_layout()
    out_path = 'fmj_plot.png'
    fig.savefig(out_path)
    logging.warning('Plot saved to %s', out_path)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='[%(levelname)s] (%(threadName)-12s) %(message)s')

    MAX_RESULTS = 30  # stop after this many successful joins

    stream_S = stream("Stream_S_Temperature", 10)
    stream_T = stream("Stream_T_Humidity", 20)
    stream_result = stream("Stream_Result", 10)

    results = []
    stats = {"dropped_over_time": []}

    t_source_S = threading.Thread(name="weather_source", target=weather_source, args=(stream_S,), daemon=True)
    t_source_T = threading.Thread(name="humidity_source", target=humidity_source, args=(stream_T,), daemon=True)
    t_fmj = threading.Thread(name="FMJ_operator", target=fuzzy_merge_join,
                             args=(stream_S, stream_T, stream_result, stats), daemon=True)

    t_source_S.start()
    t_source_T.start()
    t_fmj.start()

    # sink runs on the main thread so we can plot once it's done
    sink(stream_result, MAX_RESULTS, results)

    plot_results(results, stats["dropped_over_time"])