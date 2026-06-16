#!/usr/bin/env python3

import threading
import logging
from datetime import datetime

from stream import stream
from air_pollution_source import air_pollution_source


def tumbling_window_consumer(in_stream, window_size_seconds=10):
    """Consume an air pollution stream and emit tumbling window aggregates."""
    window_start = 0
    window_end = 0
    value_sum = 0.0
    value_count = 0

    while True:
        ts, value = in_stream.get()

        if window_start == 0:
            window_start = ts - (ts % window_size_seconds)
            window_end = window_start + window_size_seconds

        while ts >= window_end:
            if value_count > 0:
                avg_value = value_sum / value_count
                logging.info(
                    "Tumbling window [%s - %s) | Count: %d | Avg: %.2f",
                    datetime.fromtimestamp(window_start).strftime('%H:%M:%S'),
                    datetime.fromtimestamp(window_end).strftime('%H:%M:%S'),
                    value_count,
                    avg_value,
                )
            else:
                logging.info(
                    "Tumbling window [%s - %s) | empty",
                    datetime.fromtimestamp(window_start).strftime('%H:%M:%S'),
                    datetime.fromtimestamp(window_end).strftime('%H:%M:%S'),
                )

            window_start = window_end
            window_end += window_size_seconds
            value_sum = 0.0
            value_count = 0

        value_sum += value
        value_count += 1


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='[%(levelname)s] (%(threadName)-10s) %(message)s')

    pollution_stream = stream("Luftverschmutzung Stream", 20)

    pollution_thread = threading.Thread(name='air_pollution', target=air_pollution_source, args=(pollution_stream,))
    window_thread = threading.Thread(name='tumbling_window', target=tumbling_window_consumer, args=(pollution_stream, 10))

    pollution_thread.start()
    window_thread.start()
