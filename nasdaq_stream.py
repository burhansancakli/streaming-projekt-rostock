#!/usr/bin/env python3
import csv
import time
import logging
import threading
from collections import deque
from datetime import datetime

import matplotlib
matplotlib.use('TkAgg')          # change to 'Qt5Agg' / 'MacOSX' if TkAgg isn't available
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

from stream import stream


logging.basicConfig(level=logging.INFO, format='[%(levelname)s] (%(threadName)-10s) %(message)s')


# ---------------------------------------------------------------------------
# Producer – unchanged logic from original
# ---------------------------------------------------------------------------

def nasdaq_stream(oStream: stream, csvfile: str = 'NASDAQCOM.csv', day_seconds: float = 1.0):
    """Read *csvfile*, skip empty/null values and push (timestamp, price).

    Each row represents one trading day; *day_seconds* controls how many real
    seconds map to one day (default 1.0 s/day).
    """
    with open(csvfile, newline='', encoding='utf-8') as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            date_str = row.get('observation_date') or row.get('observation date')
            if not date_str:
                continue
            raw = (row.get('NASDAQCOM') or '').strip()
            if raw == '' or raw.upper() == 'NULL':
                logging.debug('skipping null/empty value for %s', date_str)
                continue
            try:
                price = float(raw)
            except ValueError:
                logging.debug('skipping invalid numeric value %r for %s', raw, date_str)
                continue
            try:
                dt = datetime.strptime(date_str, '%Y-%m-%d')
            except Exception:
                logging.debug('skipping invalid date %r', date_str)
                continue
            ts = datetime.timestamp(dt)
            oStream.put((ts, price))
            logging.info('produced %s -> %.2f', date_str, price)
            time.sleep(day_seconds)


# ---------------------------------------------------------------------------
# Consumer – collects points into shared lists for the chart thread
# ---------------------------------------------------------------------------

_lock   = threading.Lock()
_dates  = []   # datetime objects
_prices = []   # float


def collector(iStream: stream):
    """Drain the stream into the shared lists."""
    while True:
        ts, val = iStream.get()
        dt = datetime.fromtimestamp(ts)
        with _lock:
            _dates.append(dt)
            _prices.append(val)


# ---------------------------------------------------------------------------
# Chart – live update with matplotlib animation
# ---------------------------------------------------------------------------

def _moving_average(prices: list, window: int) -> list:
    """Simple trailing moving average; returns NaN for positions < window."""
    result = []
    for i in range(len(prices)):
        if i + 1 < window:
            result.append(float('nan'))
        else:
            result.append(sum(prices[i + 1 - window:i + 1]) / window)
    return result


def live_chart(ma_window: int = 20, refresh_ms: int = 500):
    """Open a matplotlib window that refreshes every *refresh_ms* ms.

    *ma_window* – number of days used for the trailing moving average.
    """
    fig, ax = plt.subplots(figsize=(12, 5))
    fig.patch.set_facecolor('#0f1117')
    ax.set_facecolor('#0f1117')

    for spine in ax.spines.values():
        spine.set_edgecolor('#333')

    ax.tick_params(colors='#aaa', which='both')
    ax.xaxis.label.set_color('#aaa')
    ax.yaxis.label.set_color('#aaa')

    line_price, = ax.plot([], [], color='#5b9cf6', linewidth=1.2,
                          label='NASDAQ Composite', zorder=2)
    line_ma,    = ax.plot([], [], color='#f6a35b', linewidth=1.8,
                          linestyle='--', label=f'{ma_window}-day MA', zorder=3)

    ax.set_title('NASDAQ Composite — live feed', color='#e0e0e0',
                 fontsize=13, pad=10)
    ax.set_xlabel('Date', labelpad=6)
    ax.set_ylabel('Index value', labelpad=6)
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y'))
    ax.xaxis.set_major_locator(mdates.YearLocator(5))
    fig.autofmt_xdate(rotation=30)

    legend = ax.legend(facecolor='#1c1e26', edgecolor='#333',
                       labelcolor='#ccc', loc='upper left')

    plt.tight_layout()

    def _update(_frame=None):
        with _lock:
            dates  = list(_dates)
            prices = list(_prices)

        if not dates:
            return

        ma = _moving_average(prices, ma_window)

        line_price.set_data(dates, prices)
        line_ma.set_data(dates, ma)

        ax.relim()
        ax.autoscale_view()
        fig.canvas.draw_idle()

    # Use matplotlib's built-in timer so we stay on the main thread
    timer = fig.canvas.new_timer(interval=refresh_ms)
    timer.add_callback(_update)
    timer.start()

    plt.show()           # blocks until the window is closed


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Live NASDAQ chart with moving average')
    parser.add_argument('--csv',       default='NASDAQCOM.csv',
                        help='Path to the NASDAQ CSV file (default: NASDAQCOM.csv)')
    parser.add_argument('--day-secs',  type=float, default=1.0,
                        help='Real seconds per simulated day (default: 1.0)')
    parser.add_argument('--ma-window', type=int,   default=20,
                        help='Moving-average window in days (default: 20)')
    parser.add_argument('--refresh',   type=int,   default=500,
                        help='Chart refresh interval in ms (default: 500)')
    args = parser.parse_args()

    st   = stream('NASDAQ', 200)

    prod = threading.Thread(name='nasdaq_producer', target=nasdaq_stream,
                            args=(st, args.csv, args.day_secs))
    cons = threading.Thread(name='collector',       target=collector,
                            args=(st,), daemon=True)

    cons.start()
    prod.start()

    # live_chart() must run on the main thread (matplotlib GUI requirement)
    live_chart(ma_window=args.ma_window, refresh_ms=args.refresh)

    prod.join()