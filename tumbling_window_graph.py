#!/usr/bin/env python3

import threading
import logging
from datetime import datetime, timedelta

import matplotlib.pyplot as plt

from stream import stream
from air_pollution_source import air_pollution_source


def tumbling_window_consumer(in_stream, window_size_seconds=10, max_windows=12):
    """Consume an air pollution stream and yield tumbling window averages as they complete."""
    window_start = 0
    window_end = 0
    value_sum = 0.0
    value_count = 0
    results = []

    while len(results) < max_windows:
        ts, value = in_stream.get()

        if window_start == 0:
            window_start = ts - (ts % window_size_seconds)
            window_end = window_start + window_size_seconds

        while ts >= window_end:
            if value_count > 0:
                avg_value = value_sum / value_count
            else:
                avg_value = 0.0

            result = (window_start, window_end, value_count, avg_value)
            results.append(result)
            yield result

            window_start = window_end
            window_end += window_size_seconds
            value_sum = 0.0
            value_count = 0

        value_sum += value
        value_count += 1

    return


def setup_live_plot(window_size_seconds=10):
    fig, ax = plt.subplots(figsize=(10, 5))
    line, = ax.plot([], [], marker='o', linestyle='-', color='tab:blue')
    ax.set_title(f'Air Pollution Tumbling Window Averages ({window_size_seconds}s windows)')
    ax.set_xlabel('Window start time')
    ax.set_ylabel('Average air pollution')
    ax.grid(True, linestyle='--', alpha=0.5)
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 100)
    fig.tight_layout()
    plt.ion()
    plt.show(block=False)
    return fig, ax, line


def update_live_plot(fig, ax, line, results):
    labels = [datetime.fromtimestamp(start).strftime('%H:%M:%S') for start, _, _, _ in results]
    averages = [avg for _, _, _, avg in results]
    x = list(range(len(results)))
    line.set_data(x, averages)
    ax.set_xticks(x)
    ax.set_xticklabels(labels, rotation=45, ha='right')
    ax.set_xlim(-0.5, max(len(results) - 0.5, 0.5))
    if averages:
        current_min = min(averages)
        current_max = max(averages)
        margin = max(1.0, (current_max - current_min) * 0.1)
        ax.set_ylim(max(0, current_min - margin), current_max + margin)
    fig.canvas.draw_idle()
    plt.pause(0.01)


def plot_window_averages(results, window_size_seconds=10):
    labels = [datetime.fromtimestamp(start).strftime('%H:%M:%S') for start, _, _, _ in results]
    averages = [avg for _, _, _, avg in results]

    plt.figure(figsize=(10, 5))
    plt.plot(labels, averages, marker='o', linestyle='-', color='tab:blue')
    plt.title(f'Air Pollution Tumbling Window Averages ({window_size_seconds}s windows)')
    plt.xlabel('Window start time')
    plt.ylabel('Average air pollution')
    plt.grid(True, linestyle='--', alpha=0.5)
    plt.tight_layout()
    plot_path = 'tumbling_window_plot.png'
    plt.savefig(plot_path)
    logging.info('Plot saved to %s', plot_path)
    try:
        plt.show()
    except Exception:
        logging.info('Plot display unavailable; open %s manually instead.', plot_path)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='[%(levelname)s] (%(threadName)-10s) %(message)s')

    pollution_stream = stream('Luftverschmutzung Stream', 50)
    pollution_thread = threading.Thread(name='air_pollution', target=air_pollution_source, args=(pollution_stream,), daemon=True)
    pollution_thread.start()

    fig, ax, line = setup_live_plot(window_size_seconds=10)
    results = []

    for result in tumbling_window_consumer(pollution_stream, window_size_seconds=10, max_windows=12):
        results.append(result)
        update_live_plot(fig, ax, line, results)
        start, end, count, avg = result
        logging.info('Window [%s - %s) | Count: %d | Avg: %.2f',
                     datetime.fromtimestamp(start).strftime('%H:%M:%S'),
                     datetime.fromtimestamp(end).strftime('%H:%M:%S'),
                     count,
                     avg)

    plot_window_averages(results, window_size_seconds=10)
    logging.info('Live update finished. Plot saved as tumbling_window_plot.png')
