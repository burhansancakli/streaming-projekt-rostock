#!/usr/bin/env python3

import threading
import logging
from datetime import datetime

import matplotlib.pyplot as plt

from stream import stream
from air_pollution_source import air_pollution_source
from sliding_window import sliding_window_consumer


def setup_live_plot(window_size=30):
    fig, ax1 = plt.subplots(figsize=(12, 6))

    ax1.set_title(f'Sliding Window Analysis (window size: {window_size}, 25s run)')
    ax1.set_xlabel('Samples (window full)')
    ax1.set_ylabel('Value')

    line_avg, = ax1.plot([], [], marker='o', linestyle='-', color='tab:blue', label='Average')
    line_min, = ax1.plot([], [], marker='v', linestyle='--', color='tab:green', label='Min', alpha=0.7)
    line_max, = ax1.plot([], [], marker='^', linestyle='--', color='tab:red', label='Max', alpha=0.7)

    ax1.legend(loc='upper left')
    ax1.grid(True, linestyle='--', alpha=0.5)

    fig.tight_layout()
    plt.ion()
    plt.show(block=False)
    return fig, ax1, line_avg, line_min, line_max


def update_live_plot(fig, ax1, line_avg, line_min, line_max, timestamps, avgs, mins, maxs):
    x = list(range(len(timestamps)))
    line_avg.set_data(x, avgs)
    line_min.set_data(x, mins)
    line_max.set_data(x, maxs)

    # Show tick labels only every ~1 second (sparse labels)
    tick_step = max(1, len(x) // 25)
    tick_positions = x[::tick_step]
    tick_labels = [timestamps[i] for i in tick_positions]
    ax1.set_xticks(tick_positions)
    ax1.set_xticklabels(tick_labels, rotation=45, ha='right', fontsize=8)
    ax1.set_xlim(-0.5, max(len(timestamps) - 0.5, 0.5))

    all_vals = mins + maxs + avgs
    if all_vals:
        current_min = min(all_vals)
        current_max = max(all_vals)
        margin = max(1.0, (current_max - current_min) * 0.15)
        ax1.set_ylim(max(0, current_min - margin), current_max + margin)

    fig.canvas.draw_idle()
    plt.pause(0.01)


def plot_final(timestamps, avgs, mins, maxs, window_size=30):
    plt.figure(figsize=(12, 6))
    x = list(range(len(timestamps)))
    plt.plot(x, avgs, marker='o', linestyle='-', color='tab:blue', label='Average')
    plt.plot(x, mins, marker='v', linestyle='--', color='tab:green', label='Min', alpha=0.7)
    plt.plot(x, maxs, marker='^', linestyle='--', color='tab:red', label='Max', alpha=0.7)

    tick_step = max(1, len(x) // 25)
    tick_positions = x[::tick_step]
    tick_labels = [timestamps[i] for i in tick_positions]
    plt.xticks(tick_positions, tick_labels, rotation=45, ha='right', fontsize=8)

    plt.title(f'Sliding Window Analysis (window size: {window_size}, 25s run)')
    plt.xlabel('Samples (window full)')
    plt.ylabel('Value')
    plt.legend()
    plt.grid(True, linestyle='--', alpha=0.5)
    plt.tight_layout()
    plot_path = 'sliding_window_plot.png'
    plt.savefig(plot_path)
    logging.info('Plot saved to %s', plot_path)
    try:
        plt.show()
    except Exception:
        logging.info('Plot display unavailable; open %s manually instead.', plot_path)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='[%(levelname)s] (%(threadName)-10s) %(message)s')

    window_size = 30
    run_seconds = 25

    pollution_stream = stream('Air Pollution Stream', 100)
    pollution_thread = threading.Thread(
        name='air_pollution',
        target=air_pollution_source,
        args=(pollution_stream,),
        daemon=True
    )
    pollution_thread.start()

    fig, ax1, line_avg, line_min, line_max = setup_live_plot(window_size=window_size)

    timestamps = []
    avgs = []
    mins_list = []
    maxs_list = []

    import time
    start_time = time.time()

    for result in sliding_window_consumer(pollution_stream, window_size=window_size):
        oldest_ts, newest_ts, count, avg_value, min_value, max_value = result

        label = datetime.fromtimestamp(newest_ts).strftime('%H:%M:%S')
        timestamps.append(label)
        avgs.append(avg_value)
        mins_list.append(min_value)
        maxs_list.append(max_value)

        update_live_plot(fig, ax1, line_avg, line_min, line_max, timestamps, avgs, mins_list, maxs_list)

        logging.info(
            'Window [%s - %s] | Count: %d | Avg: %.2f | Min: %.2f | Max: %.2f',
            datetime.fromtimestamp(oldest_ts).strftime('%H:%M:%S'),
            datetime.fromtimestamp(newest_ts).strftime('%H:%M:%S'),
            count, avg_value, min_value, max_value
        )

        if time.time() - start_time >= run_seconds:
            break

    plot_final(timestamps, avgs, mins_list, maxs_list, window_size=window_size)
    logging.info('Live update finished after %ds. Plot saved as sliding_window_plot.png', run_seconds)
