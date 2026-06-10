#!/usr/bin/env python3

import threading
import time
import random
import logging
from datetime import datetime

from strthread import stream


# ─────────────────────────────────────────
# DATA SOURCES
# Two simulated sensors emitting tuples at different rates.
# Each tuple = (timestamp, value)
# In a real scenario: S = PM2.5 sensor, T = PM10 sensor
# ─────────────────────────────────────────

def fountain_slow(oStream):
    """
    Slow source — emits one tuple every ~1.5 seconds.
    This is stream S, the DOMINANT stream.
    It has a lower frequency, so it controls the join rhythm.
    Every tuple from S will produce exactly one join result.
    """
    counter = 0
    while True:
        ts = datetime.timestamp(datetime.now())
        value = random.randint(0, 100)
        t = (ts, value)
        oStream.put(t)
        logging.info("[SLOW/S] Emitted: ts=%.3f, val=%d" % (ts, value))
        counter += 1
        time.sleep(1.5 + random.random() * 0.5)  # wait ~1.5–2 seconds before next tuple


def fountain_fast(oStream):
    """
    Fast source — emits one tuple every ~0.4 seconds.
    This is stream T, the RECESSIVE (dominated) stream.
    It has a higher frequency, so some tuples will be discarded
    if they arrive before the current dominant tuple from S.
    """
    while True:
        ts = datetime.timestamp(datetime.now())
        value = random.randint(0, 100)
        t = (ts, value)
        oStream.put(t)
        logging.info("[FAST/T] Emitted: ts=%.3f, val=%d" % (ts, value))
        time.sleep(0.4 + random.random() * 0.2)  # wait ~0.4–0.6 seconds before next tuple


# ─────────────────────────────────────────
# FUZZY MERGE JOIN OPERATOR
#
# How it works:
#   S emits slowly  → one tuple every ~1.5s  (dominant)
#   T emits quickly → one tuple every ~0.4s  (recessive)
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
    Fuzzy Merge Join between stream S (dominant) and stream T (recessive).

    Parameters:
        iStream_S  — the slow input stream (dominant)
        iStream_T  — the fast input stream (recessive)
        oStream    — output stream where joined tuples are placed
    """

    join_count = 0    # counts how many successful joins have been made
    dropped_count = 0 # counts how many T tuples were discarded

    while True:

        # ── STEP 1: Wait for the next tuple from S ──────────────────────────
        # get() automatically BLOCKS if the stream is empty.
        # The thread sleeps here until source_S puts a new tuple in.
        # This is why we see "get blocked by empty stream" in the logs — it is normal.
        tuple_S = iStream_S.get()
        ts_S  = tuple_S[0]  # extract timestamp from S tuple
        val_S = tuple_S[1]  # extract sensor value from S tuple

        logging.info("[FMJ] Received from S: ts=%.3f, val=%d" % (ts_S, val_S))
        logging.info("[FMJ] Looking for T partner with ts >= %.3f ..." % ts_S)

        # ── STEP 2: Search for a valid join partner in T ─────────────────────
        # Keep consuming T tuples until we find one with timestamp >= ts_S.
        # T tuples that arrived earlier than S are outdated and get discarded.
        found_partner = False

        while not found_partner:

            # get() also blocks here if Stream_T is temporarily empty.
            # The thread waits until source_T emits the next tuple.
            tuple_T = iStream_T.get()
            ts_T  = tuple_T[0]  # extract timestamp from T tuple
            val_T = tuple_T[1]  # extract sensor value from T tuple

            if ts_T < ts_S:
                # This T tuple is too old — its timestamp is earlier than S.
                # Discard it. It cannot be a partner for this S tuple,
                # and it will not be useful for any future S tuple either
                # (because S timestamps only increase over time).
                dropped_count += 1
                logging.info("[FMJ] Discarded T: ts=%.3f (need >= %.3f). "
                             "Total discarded: %d" % (ts_T, ts_S, dropped_count))
                # loop continues → pick the next T tuple

            else:
                # Found a valid partner! ts_T >= ts_S
                found_partner = True

                # ── STEP 3: Build and emit the join result ───────────────────
                # We combine: timestamp from S, value from S, value from T.
                # The timestamp of T is ignored — S is the reference (dominant).
                result_tuple = (ts_S, val_S, val_T)

                oStream.put(result_tuple)  # send result downstream to sink
                join_count += 1

                logging.warning("[FMJ] ✓ JOIN #%d: S(ts=%.3f, val=%d) + "
                                "T(ts=%.3f, val=%d) → result: %s"
                                % (join_count, ts_S, val_S, ts_T, val_T,
                                   str(result_tuple)))


# ─────────────────────────────────────────
# SINK OPERATOR
# Consumes join results and displays them.
# In a real project: this is where you would save to a file or database.
# ─────────────────────────────────────────

def sink(iStream):
    """
    Sink — reads joined tuples from the result stream and prints them.

    Parameters:
        iStream — the result stream produced by the join operator
    """
    results = []  # keeps all results in memory for counting

    while True:
        result = iStream.get()          # blocks until a result is available
        results.append(result)
        ts, val_s, val_t = result       # unpack the 3-element result tuple

        logging.warning("[SINK] Join result: timestamp=%.3f | "
                        "val_S=%d | val_T=%d | "
                        "Total results so far: %d"
                        % (ts, val_s, val_t, len(results)))


# ─────────────────────────────────────────
# MAIN — wire everything together and start
#
# The pipeline looks like this:
#
#   fountain_slow ──→ [Stream_S] ──→ ┐
#                                     ├──→ fuzzy_merge_join ──→ [Stream_Result] ──→ sink
#   fountain_fast ──→ [Stream_T] ──→ ┘
#
# Each box [ ] is a thread-safe queue (circular buffer from strthread.py).
# Each function runs in its own thread, in parallel.
# ─────────────────────────────────────────

if __name__ == "__main__":

    logging.basicConfig(
        level=logging.INFO,
        format='[%(levelname)s] (%(threadName)-12s) %(message)s'
    )

    logging.warning("=" * 60)
    logging.warning("  FUZZY MERGE JOIN — starting...")
    logging.warning("  S = slow stream (dominant)   ~1.5 sec/tuple")
    logging.warning("  T = fast stream (recessive)  ~0.4 sec/tuple")
    logging.warning("=" * 60)

    # Create the stream queues (thread-safe circular buffers from strthread.py)
    # winsize = maximum number of tuples that can wait in the queue at once
    # If the queue is full, put() blocks. If empty, get() blocks. Automatic.
    stream_S      = stream("Stream_S_slow", winsize=10)
    stream_T      = stream("Stream_T_fast", winsize=20)  # larger because T is fast
    stream_result = stream("Stream_Result", winsize=10)

    # Create threads — one per operator
    # Each thread runs its target function independently and in parallel
    t_source_S = threading.Thread(
        name="source_S",
        target=fountain_slow,   # this function runs in this thread
        args=(stream_S,)        # stream_S is passed as argument
    )

    t_source_T = threading.Thread(
        name="source_T",
        target=fountain_fast,
        args=(stream_T,)
    )

    t_fmj = threading.Thread(
        name="FMJ_operator",
        target=fuzzy_merge_join,
        args=(stream_S, stream_T, stream_result)  # reads from S and T, writes to result
    )

    t_sink = threading.Thread(
        name="sink",
        target=sink,
        args=(stream_result,)  # reads from result stream
    )

    # Start threads — consumers first, then producers.
    # This avoids a race condition where a producer emits before the consumer is ready.
    t_sink.start()
    t_fmj.start()
    t_source_T.start()
    t_source_S.start()  

    logging.warning("All threads started. Press Ctrl+C to stop.")