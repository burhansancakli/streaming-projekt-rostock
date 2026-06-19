#!/usr/bin/env python3

import threading
import time
from datetime import datetime
import random
import logging

from stream import stream
from sensor_energy_source import sensor_energy_source

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

            logging.info(
                "Landmark Window [%s - %s] | Count: %d | Total energy: %.4f Wh | Avg energy: %.4f Wh",
                lm_time,
                curr_time,
                window_count,
                window_sum,
                avg_energy
            )
        else:
            logging.debug("Tuple dropped (older than landmark)")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='[%(levelname)s] (%(threadName)-10s) %(message)s')

    # Using energy consumption stream for this demo
    energy_stream = stream("Sensor Energy Stream", 10)

    # Threads
    energy_thread = threading.Thread(name='sensor_energy', target=sensor_energy_source, args=(energy_stream,))
    consumer_thread = threading.Thread(name='window_consumer', target=landmark_window_consumer, args=(energy_stream,))

    energy_thread.start()
    consumer_thread.start()
