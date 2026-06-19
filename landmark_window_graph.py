#!/usr/bin/env python3

import threading
import logging
from datetime import datetime

import matplotlib.pyplot as plt

from stream import stream
from sensor_energy_source import sensor_energy_source


def landmark_window_consumer(in_stream, max_points=50):
    """Consume an energy stream and yield running landmark-window averages as tuples.
    Yields (landmark_ts, current_ts, count, avg)
    """
    landmark = None
    window_sum = 0.0
    window_count = 0
    results = []

    while len(results) < max_points:
        ts, value = in_stream.get()

        if landmark is None:
            landmark = ts - (ts % 1)  # normalize to second

        if ts >= landmark:
            window_sum += value
            window_count += 1
            avg = window_sum / window_count if window_count > 0 else 0.0
            result = (landmark, ts, window_count, avg)
            results.append(result)
            yield result
        else:
            logging.debug('Tuple ignored (older than landmark)')

    return


def setup_live_plot():
    fig, ax = plt.subplots(figsize=(10, 5))
    line, = ax.plot([], [], marker='o', linestyle='-', color='tab:green')
    ax.set_title('Sensor Energy Landmark Window (cumulative average)')
    ax.set_xlabel('Time')
    ax.set_ylabel('Average energy (Wh)')
    ax.grid(True, linestyle='--', alpha=0.5)
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    fig.tight_layout()
    plt.ion()
    plt.show(block=False)
    return fig, ax, line


def update_live_plot(fig, ax, line, results):
    labels = [datetime.fromtimestamp(ts).strftime('%H:%M:%S') for _, ts, _, _ in results]
    averages = [avg for _, _, _, avg in results]
    x = list(range(len(results)))
    line.set_data(x, averages)
    ax.set_xticks(x)
    ax.set_xticklabels(labels, rotation=45, ha='right')
    ax.set_xlim(-0.5, max(len(results) - 0.5, 0.5))
    if averages:
        current_min = min(averages)
        current_max = max(averages)
        margin = max(0.001, (current_max - current_min) * 0.1)
        ax.set_ylim(max(0, current_min - margin), current_max + margin)
    fig.canvas.draw_idle()
    plt.pause(0.01)


def plot_window_averages(results):
    labels = [datetime.fromtimestamp(ts).strftime('%H:%M:%S') for _, ts, _, _ in results]
    averages = [avg for _, _, _, avg in results]

    plt.figure(figsize=(10, 5))
    plt.plot(labels, averages, marker='o', linestyle='-', color='tab:green')
    plt.title('Sensor Energy Landmark Window (cumulative average)')
    plt.xlabel('Time')
    plt.ylabel('Average energy (Wh)')
    plt.grid(True, linestyle='--', alpha=0.5)
    plt.tight_layout()
    plot_path = 'landmark_window_plot.png'
    plt.savefig(plot_path)
    logging.info('Plot saved to %s', plot_path)
    try:
        plt.show()
    except Exception:
        logging.info('Plot display unavailable; open %s manually instead.', plot_path)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='[%(levelname)s] (%(threadName)-10s) %(message)s')

    energy_stream = stream('Sensor Energy Stream', 10)
    energy_thread = threading.Thread(name='sensor_energy', target=sensor_energy_source, args=(energy_stream,), daemon=True)
    energy_thread.start()

    fig, ax, line = setup_live_plot()
    results = []

    for result in landmark_window_consumer(energy_stream, max_points=10):
        results.append(result)
        update_live_plot(fig, ax, line, results)
        start, ts, count, avg = result
        logging.info('Landmark %s | Current %s | Count: %d | Avg: %.4f Wh',
                     datetime.fromtimestamp(start).strftime('%H:%M:%S'),
                     datetime.fromtimestamp(ts).strftime('%H:%M:%S'),
                     count,
                     avg)

    plot_window_averages(results)
    logging.info('Live update finished. Plot saved as landmark_window_plot.png')
