#!/usr/bin/env python3

import threading
import time
import argparse
from datetime import datetime
import random
import logging

from stream import stream
from sensor_energy_source import sensor_energy_source
from air_pollution_source import air_pollution_source
from weather_source import weather_source
from humidity_source import humidity_source

def landmark_window_consumer(in_stream):
    """
    A consumer that implements a landmark window.
    The landmark is the timestamp of the first tuple.
    The window includes all tuples with timestamps >= landmark.
    """
    landmark = None
    window_sum = 0.0
    window_count = 0

    while True:
        item = in_stream.get()
        ts, value = item

        if landmark is None:
            landmark = ts
            logging.info("Landmark window initialized at timestamp: %s", datetime.fromtimestamp(landmark).strftime('%H:%M:%S.%f'))

        # In a landmark window, we consider all tuples from the landmark onwards
        if ts >= landmark:
            window_sum += value
            window_count += 1
            avg_energy = window_sum / window_count if window_count > 0 else 0.0

            lm_time = datetime.fromtimestamp(landmark).strftime('%H:%M:%S')
            curr_time = datetime.fromtimestamp(ts).strftime('%H:%M:%S')

            result = (landmark, ts, window_count, window_sum, avg_energy)
            logging.info(
                "Landmark Window [%s - %s] | Count: %d | Total energy: %.4f Wh | Avg energy: %.4f Wh",
                lm_time,
                curr_time,
                window_count,
                window_sum,
                avg_energy
            )
            yield result
        else:
            logging.debug("Tuple dropped (older than landmark)")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Landmark window operator for stream processing')
    parser.add_argument('--source', type=str, default='sensor_energy',
                        choices=['air_pollution', 'weather', 'humidity', 'sensor_energy'],
                        help='Data source to consume from (default: sensor_energy)')
    parser.add_argument('--stream-size', type=int, default=40,
                        help='Internal stream buffer size (default: 40)')
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
    energy_stream = stream(stream_name, args.stream_size)

    energy_thread = threading.Thread(name=args.source, target=source_func, args=(energy_stream,), daemon=True)
    energy_thread.start()

    for result in landmark_window_consumer(energy_stream):
        pass  # results are logged inside the consumer
