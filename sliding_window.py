#!/usr/bin/env python3
"""Sliding window operator for stream processing."""

import threading
import logging
import argparse
from datetime import datetime
from collections import deque

from stream import stream
from nasdaq_source import nasdaq_source
from air_pollution_source import air_pollution_source
from weather_source import weather_source
from humidity_source import humidity_source
from sensor_energy_source import sensor_energy_source


def sliding_window_consumer(in_stream, window_size=10):
    """Consume a stream and maintain a sliding window of the last *window_size* tuples.

    Yields a result tuple on every incoming tuple:
        (oldest_ts, newest_ts, count, avg_value, min_value, max_value)

    Parameters:
        in_stream:      input stream
        window_size:    number of tuples in the sliding window (default 10)
    """
    window = deque(maxlen=window_size)

    while True:
        ts, value = in_stream.get()
        window.append((ts, value))

        values = [v for _, v in window]
        count = len(values)
        avg_value = sum(values) / count
        min_value = min(values)
        max_value = max(values)
        oldest_ts = window[0][0]
        newest_ts = window[-1][0]

        result = (oldest_ts, newest_ts, count, avg_value, min_value, max_value)

        logging.info(
            "Sliding Window [%s - %s] | Count: %d | Avg: %.2f | Min: %.2f | Max: %.2f",
            datetime.fromtimestamp(oldest_ts).strftime('%H:%M:%S'),
            datetime.fromtimestamp(newest_ts).strftime('%H:%M:%S'),
            count,
            avg_value,
            min_value,
            max_value,
        )
        yield result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Sliding window operator for stream processing')
    parser.add_argument('--source', type=str, default='nasdaq',
                        choices=['nasdaq', 'air_pollution', 'weather', 'humidity', 'sensor_energy'],
                        help='Data source to consume from (default: nasdaq)')
    parser.add_argument('--window-size', type=int, default=10,
                        help='Number of tuples in the sliding window (default: 10)')
    parser.add_argument('--stream-size', type=int, default=20,
                        help='Internal stream buffer size (default: 20)')
    parser.add_argument('--csv', type=str, default='NASDAQCOM.csv',
                        help='Path to NASDAQ CSV file (only for nasdaq source)')
    parser.add_argument('--day-secs', type=float, default=0.2,
                        help='Real seconds per simulated day (only for nasdaq source)')
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format='[%(levelname)s] (%(threadName)-10s) %(message)s')

    sources = {
        'nasdaq': ('NASDAQ', lambda oS: nasdaq_source(oS, csvfile=args.csv, day_seconds=args.day_secs)),
        'air_pollution': ('Air Pollution', air_pollution_source),
        'weather': ('Weather', weather_source),
        'humidity': ('Humidity', humidity_source),
        'sensor_energy': ('Sensor Energy', sensor_energy_source),
    }

    stream_name, source_func = sources[args.source]
    data_stream = stream(stream_name, args.stream_size)

    source_thread = threading.Thread(name=args.source, target=source_func, args=(data_stream,), daemon=True)
    source_thread.start()

    for result in sliding_window_consumer(data_stream, window_size=args.window_size):
        pass  # results are logged inside the consumer
