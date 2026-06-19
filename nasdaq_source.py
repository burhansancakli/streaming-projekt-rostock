#!/usr/bin/env python3
"""NASDAQ Composite data source for stream processing benchmarks."""

import csv
import time
import logging
from datetime import datetime

from stream import stream


def nasdaq_source(oStream: stream, csvfile: str = 'NASDAQCOM.csv', day_seconds: float = 0.2):
    """Read NASDAQ CSV data and push (timestamp, price) tuples into the stream.

    Skips rows with empty/null values.

    Parameters:
        oStream:        output stream
        csvfile:        path to the NASDAQ CSV file
        day_seconds:    real seconds per simulated trading day (default 0.2)
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
            oStream.put_force((ts, price))
            logging.info('produced %s -> %.2f', date_str, price)
            time.sleep(day_seconds)
