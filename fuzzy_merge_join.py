#!/usr/bin/env python3

import threading
import time
import random
import logging
import argparse

from weather_source import weather_source
from humidity_source import humidity_source
from air_pollution_source import air_pollution_source
from sensor_energy_source import sensor_energy_source
from stream import stream


# ─────────────────────────────────────────
# DATA SOURCES
#
# S = Temperature  (weather_source.py)   — ~1.0-2.0s / tuple  → DOMINANT (slower stream)
# T = Humidity     (humidity_source.py)  — ~0.4-0.6s / tuple  → RECESSIVE (faster stream)
#
# Each tuple = (timestamp, value)
# ─────────────────────────────────────────


# ─────────────────────────────────────────
# FUZZY MERGE JOIN OPERATOR
#
# How it works:
#   S (Temperature) emits slowly  → dominant
#   T (Humidity)    emits quickly → recessive
#
# For each tuple from S:
#   → consume tuples from T until we find one with timestamp >= timestamp_S
#   → any T tuple that arrives before S is discarded (too old, no future use)
#   → combine S and T into a result tuple and send it downstream
#
# Why discard old T tuples?
#   Because S timestamps are always increasing. If T(0.3) is too old for S(1.0),
#   it will also be too old for S(2.0), S(3.0), etc. So it is safe to throw it away.
# ─────────────────────────────────────────

def fuzzy_merge_join(iStream_S, iStream_T, oStream):
    """
    Fuzzy Merge Join between stream S = Temperature (dominant)
    and stream T = Humidity (recessive).

    Parameters:
        iStream_S  — Temperature stream (dominant, slower)
        iStream_T  — Humidity stream (recessive, faster)
        oStream    — output stream where joined tuples are placed
    """

    join_count = 0     # counts how many successful joins have been made
    dropped_count = 0  # counts how many Humidity tuples were discarded

    while True:

        # ── STEP 1: Wait for the next Temperature tuple ─────────────────────
        tuple_S = iStream_S.get()
        ts_S = tuple_S[0]    # timestamp
        val_S = tuple_S[1]   # temperature value

        logging.info("[FMJ] Received Temperature: ts=%.3f, val=%.2f" % (ts_S, val_S))
        logging.info("[FMJ] Looking for Humidity partner with ts >= %.3f ..." % ts_S)

        # ── STEP 2: Search for a valid join partner in Humidity ─────────────
        # Keep consuming Humidity tuples until we find one with timestamp >= ts_S.
        # Humidity tuples that arrived earlier than the Temperature tuple are
        # outdated and get discarded.
        found_partner = False

        while not found_partner:

            tuple_T = iStream_T.get()
            ts_T = tuple_T[0]    # timestamp
            val_T = tuple_T[1]   # humidity value

            if ts_T < ts_S:
                # Too old — discard. It cannot be a partner for this Temperature
                # tuple, and it will not be useful for any future one either
                # (Temperature timestamps only increase).
                dropped_count += 1
                logging.info("[FMJ] Discarded Humidity: ts=%.3f (need >= %.3f). "
                             "Total discarded: %d" % (ts_T, ts_S, dropped_count))
                # loop continues -> pick the next Humidity tuple

            else:
                # Found a valid partner: ts_T >= ts_S
                found_partner = True

                # ── STEP 3: Build and emit the join result ───────────────────
                # (timestamp from S, temperature value, humidity value)
                result_tuple = (ts_S, val_S, val_T)

                oStream.put_force(result_tuple)
                join_count += 1

                logging.warning("[FMJ] JOIN #%d: Temp(ts=%.3f, val=%.2f) + "
                                "Humidity(ts=%.3f, val=%.2f) -> result: %s"
                                % (join_count, ts_S, val_S, ts_T, val_T,
                                   str(result_tuple)))


# ─────────────────────────────────────────
# SINK OPERATOR
# Consumes join results and displays them.
# ─────────────────────────────────────────

def sink(iStream):
    """
    Sink — reads joined tuples from the result stream and prints them.
    """
    results = []  # keeps all results in memory for counting

    while True:
        result = iStream.get()
        results.append(result)
        ts, val_temp, val_humidity = result

        logging.warning("[SINK] Join result: timestamp=%.3f | "
                        "temperature=%.2f | humidity=%.2f | "
                        "Total results so far: %d"
                        % (ts, val_temp, val_humidity, len(results)))


# ─────────────────────────────────────────
# MAIN — wire everything together and start
#
# The pipeline looks like this:
#
#   weather_source  ──→ [Stream_S = Temperature] ──→ ┐
#                                                       ├──→ fuzzy_merge_join ──→ [Stream_Result] ──→ sink
#   humidity_source ──→ [Stream_T = Humidity]    ──→ ┘
#
# ─────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Fuzzy Merge Join operator')
    parser.add_argument('--source-s', type=str, default='weather',
                        choices=['air_pollution', 'weather', 'humidity', 'sensor_energy'],
                        help='Dominant stream S (slower) source (default: weather)')
    parser.add_argument('--source-t', type=str, default='humidity',
                        choices=['air_pollution', 'weather', 'humidity', 'sensor_energy'],
                        help='Recessive stream T (faster) source (default: humidity)')
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format='[%(levelname)s] (%(threadName)-12s) %(message)s'
    )

    sources = {
        'air_pollution': ('Air Pollution', air_pollution_source),
        'weather': ('Weather', weather_source),
        'humidity': ('Humidity', humidity_source),
        'sensor_energy': ('Sensor Energy', sensor_energy_source),
    }

    source_s_name, source_s_func = sources[args.source_s]
    source_t_name, source_t_func = sources[args.source_t]

    logging.warning("=" * 60)
    logging.warning(f"  FUZZY MERGE JOIN — {source_s_name} x {source_t_name}")
    logging.warning(f"  S = {source_s_name} (dominant)   → SLOWER STREAM")
    logging.warning(f"  T = {source_t_name} (recessive) → FASTER STREAM")
    logging.warning("=" * 60)

    stream_S = stream(f"Stream_S_{source_s_name}", 10)
    stream_T = stream(f"Stream_T_{source_t_name}", 20)
    stream_result = stream("Stream_Result", 10)

    t_source_S = threading.Thread(
        name="source_S",
        target=source_s_func,
        args=(stream_S,)
    )

    t_source_T = threading.Thread(
        name="source_T",
        target=source_t_func,
        args=(stream_T,)
    )

    t_fmj = threading.Thread(
        name="FMJ_operator",
        target=fuzzy_merge_join,
        args=(stream_S, stream_T, stream_result)
    )

    t_sink = threading.Thread(
        name="sink",
        target=sink,
        args=(stream_result,)
    )

    t_sink.start()
    t_fmj.start()
    t_source_T.start()
    t_source_S.start()

    logging.warning("All threads started. Press Ctrl+C to stop.")