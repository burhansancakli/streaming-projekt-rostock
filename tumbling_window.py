#!/usr/bin/env python3

import threading
import logging
import argparse
from datetime import datetime

from stream import stream
from air_pollution_source import air_pollution_source
from weather_source import weather_source
from humidity_source import humidity_source
from sensor_energy_source import sensor_energy_source


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
                result = (window_start, window_end, value_count, avg_value)
                logging.info(
                    "Tumbling window [%s - %s) | Count: %d | Avg: %.2f",
                    datetime.fromtimestamp(window_start).strftime('%H:%M:%S'),
                    datetime.fromtimestamp(window_end).strftime('%H:%M:%S'),
                    value_count,
                    avg_value,
                )
                yield result
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
    parser = argparse.ArgumentParser(description='Tumbling window operator for stream processing')
    parser.add_argument('--source', type=str, default='air_pollution', 
                        choices=['air_pollution', 'weather', 'humidity', 'sensor_energy'],
                        help='Data source to consume from (default: air_pollution)')
    parser.add_argument('--window-size', type=int, default=10,
                        help='Window size in seconds (default: 10)')
    parser.add_argument('--stream-size', type=int, default=20,
                        help='Internal stream buffer size (default: 20)')
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format='[%(levelname)s] (%(threadName)-10s) %(message)s')

    # Map source names to source functions
    sources = {
        'air_pollution': ('Air Pollution Stream', air_pollution_source),
        'weather': ('Weather Stream', weather_source),
        'humidity': ('Humidity Stream', humidity_source),
        'sensor_energy': ('Sensor Energy Stream', sensor_energy_source),
    }

    stream_name, source_func = sources[args.source]
    data_stream = stream(stream_name, args.stream_size)

    source_thread = threading.Thread(name=args.source, target=source_func, args=(data_stream,), daemon=True)
    source_thread.start()

    for result in tumbling_window_consumer(data_stream, window_size_seconds=args.window_size):
        pass  # results are logged inside the consumer
