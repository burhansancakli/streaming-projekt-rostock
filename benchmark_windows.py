#!/usr/bin/env python3

import threading
import time
import argparse
import logging
import csv
import sys
from datetime import datetime

import psutil
import matplotlib.pyplot as plt

from stream import stream
from air_pollution_source import air_pollution_source
from weather_source import weather_source
from humidity_source import humidity_source
from sensor_energy_source import sensor_energy_source
from nasdaq_source import nasdaq_source


class StatsCollector:
    """Collects memory, CPU, and throughput statistics during execution."""
    
    TUPLE_OVERHEAD = 64  # approximate bytes per (timestamp, value) tuple
    
    def __init__(self, operator_name, process, streams=None):
        self.operator_name = operator_name
        self.process = process
        self.streams = streams or []
        self.window_state = {'tuples_in_window': 0, 'window_memory_bytes': 0}
        self.stats = {
            'timestamps': [],
            'memory_mb': [],
            'stream_memory_bytes': [],
            'stream_occupancy': [],
            'window_tuples': [],
            'window_memory_bytes': [],
            'cpu_percent': [],
            'tuple_count': [],
        }
        self.tuple_count = 0
        self.lock = threading.Lock()
        self.start_time = time.time()
        self.running = True
    
    def increment_tuples(self):
        with self.lock:
            self.tuple_count += 1
    
    def _compute_stream_memory(self):
        """Compute total memory used by all tracked streams in bytes."""
        total = 0
        occupancy = {}
        for s in self.streams:
            occupancy[s._name] = s._cnt
            # memory = buffer array + tuple objects
            total += sys.getsizeof(s._stream)
            total += s._cnt * self.TUPLE_OVERHEAD
        return total, occupancy
    
    def sample_metrics(self):
        """Collect a single sample of metrics."""
        try:
            memory_info = self.process.memory_info()
            memory_mb = memory_info.rss / (1024 * 1024)
            cpu_percent = self.process.cpu_percent(interval=0.1)
            stream_mem, occupancy = self._compute_stream_memory()
            
            elapsed = time.time() - self.start_time
            with self.lock:
                self.stats['timestamps'].append(elapsed)
                self.stats['memory_mb'].append(memory_mb)
                self.stats['stream_memory_bytes'].append(stream_mem)
                self.stats['stream_occupancy'].append(occupancy)
                self.stats['window_tuples'].append(self.window_state['tuples_in_window'])
                self.stats['window_memory_bytes'].append(self.window_state['window_memory_bytes'])
                self.stats['cpu_percent'].append(cpu_percent)
                self.stats['tuple_count'].append(self.tuple_count)
        except Exception as e:
            logging.warning(f"Failed to sample metrics: {e}")
    
    def monitoring_thread(self, interval=0.5):
        """Background thread that periodically samples metrics."""
        while self.running:
            self.sample_metrics()
            time.sleep(interval)
    
    def stop(self):
        self.running = False
    
    def get_summary(self):
        """Return aggregated statistics."""
        if not self.stats['memory_mb']:
            return {}
        
        peak_stream_mem = max(self.stats['stream_memory_bytes'])
        avg_stream_mem = sum(self.stats['stream_memory_bytes']) / len(self.stats['stream_memory_bytes'])
        
        peak_window_mem = max(self.stats['window_memory_bytes'])
        avg_window_mem = sum(self.stats['window_memory_bytes']) / len(self.stats['window_memory_bytes'])
        
        # Peak occupancy across all streams
        peak_occupancy = {}
        for occ in self.stats['stream_occupancy']:
            for name, count in occ.items():
                peak_occupancy[name] = max(peak_occupancy.get(name, 0), count)
        
        return {
            'operator': self.operator_name,
            'peak_memory_mb': max(self.stats['memory_mb']),
            'avg_memory_mb': sum(self.stats['memory_mb']) / len(self.stats['memory_mb']),
            'final_memory_mb': self.stats['memory_mb'][-1],
            'peak_stream_memory_kb': peak_stream_mem / 1024,
            'avg_stream_memory_kb': avg_stream_mem / 1024,
            'peak_window_memory_bytes': peak_window_mem,
            'avg_window_memory_bytes': avg_window_mem,
            'peak_occupancy': peak_occupancy,
            'avg_cpu_percent': sum(self.stats['cpu_percent']) / len(self.stats['cpu_percent']) if self.stats['cpu_percent'] else 0,
            'total_tuples': self.tuple_count,
            'duration_seconds': time.time() - self.start_time,
        }


def _tumbling_window_benchmark(in_stream, window_size_seconds, collector, results_count, stop_event):
    """Tumbling window consumer with window state tracking for benchmarking."""
    from datetime import datetime as dt
    window_start = 0
    window_end = 0
    value_sum = 0.0
    value_count = 0

    while not stop_event.is_set():
        ts, value = in_stream.get()

        if window_start == 0:
            window_start = ts - (ts % window_size_seconds)
            window_end = window_start + window_size_seconds

        while ts >= window_end:
            if value_count > 0:
                avg_value = value_sum / value_count
                result = (window_start, window_end, value_count, avg_value)
                # Window completed — reset accumulator
                collector.window_state['tuples_in_window'] = 0
                collector.window_state['window_memory_bytes'] = 0
                collector.increment_tuples()
                results_count['count'] += 1
                yield result

            window_start = window_end
            window_end += window_size_seconds
            value_sum = 0.0
            value_count = 0

        value_sum += value
        value_count += 1
        # Update window state — tuples accumulating in current window
        collector.window_state['tuples_in_window'] = value_count
        collector.window_state['window_memory_bytes'] = value_count * StatsCollector.TUPLE_OVERHEAD


def _landmark_window_benchmark(in_stream, collector, results_count, stop_event):
    """Landmark window consumer with window state tracking for benchmarking."""
    from datetime import datetime as dt
    landmark = None
    window_sum = 0.0
    window_count = 0

    while not stop_event.is_set():
        item = in_stream.get()
        ts, value = item

        if landmark is None:
            landmark = ts

        if ts >= landmark:
            window_sum += value
            window_count += 1
            avg_energy = window_sum / window_count if window_count > 0 else 0.0
            result = (landmark, ts, window_count, window_sum, avg_energy)
            # Update window state — accumulates forever
            collector.window_state['tuples_in_window'] = window_count
            collector.window_state['window_memory_bytes'] = window_count * StatsCollector.TUPLE_OVERHEAD
            collector.increment_tuples()
            results_count['count'] += 1
            yield result


def _sliding_window_benchmark(in_stream, window_size, collector, results_count, stop_event):
    """Sliding window consumer with window state tracking for benchmarking."""
    from collections import deque
    window = deque(maxlen=window_size)

    while not stop_event.is_set():
        ts, value = in_stream.get()
        window.append((ts, value))

        values = [v for _, v in window]
        count = len(values)
        avg_value = sum(values) / count
        result = (window[0][0], window[-1][0], count, avg_value, min(values), max(values))

        # Update window state — grows until window_size, then stays constant
        collector.window_state['tuples_in_window'] = count
        collector.window_state['window_memory_bytes'] = count * StatsCollector.TUPLE_OVERHEAD
        collector.increment_tuples()
        results_count['count'] += 1
        yield result


def benchmark_operator(operator_name, consumer_func, source_func, source_name, 
                       stream_size=20, window_size=10, duration=60):
    """
    Run a single operator for a fixed duration with stats collection.
    Returns (StatsCollector, results_count)
    """
    process = psutil.Process()

    data_stream = stream(source_name, stream_size)
    collector = StatsCollector(operator_name, process, streams=[data_stream])
    results_count = {'count': 0}
    stop_event = threading.Event()
    
    # Wrap data_stream.get() to check stop_event periodically
    def guarded_get():
        while not stop_event.is_set():
            data_stream._mutex.acquire()
            if data_stream._isempty():
                data_stream._mutex.wait(timeout=0.5)
                # Re-check after waking (may have data now or may have timed out)
                if data_stream._isempty():
                    data_stream._mutex.release()
                    continue
            t = data_stream._dequeue()
            data_stream._mutex.notify()
            data_stream._mutex.release()
            return t
        raise RuntimeError("Benchmark stopped")
    data_stream.get = guarded_get
    
    def instrumented_consumer():
        """Run the consumer with window state tracking."""
        try:
            if operator_name == 'Tumbling Window':
                for _ in _tumbling_window_benchmark(data_stream, window_size, collector, results_count, stop_event):
                    pass
            elif operator_name == 'Sliding Window':
                for _ in _sliding_window_benchmark(data_stream, window_size, collector, results_count, stop_event):
                    pass
            else:
                for _ in _landmark_window_benchmark(data_stream, collector, results_count, stop_event):
                    pass
        except RuntimeError as e:
            if "Benchmark stopped" in str(e):
                logging.debug(f"Consumer stopped: {e}")
            else:
                logging.debug(f"Consumer exception: {e}")
        except Exception as e:
            logging.debug(f"Consumer exception: {e}")
    
    # Timer thread: stops the benchmark after `duration` seconds
    def timer():
        time.sleep(duration)
        stop_event.set()
    timer_thread = threading.Thread(name=f'{operator_name}_timer', target=timer, daemon=True)
    timer_thread.start()
    
    # Start monitoring thread
    monitor_thread = threading.Thread(name=f'{operator_name}_monitor', 
                                      target=collector.monitoring_thread, daemon=True)
    monitor_thread.start()
    
    # Start source thread
    source_thread = threading.Thread(name='source', target=source_func, 
                                    args=(data_stream,), daemon=True)
    source_thread.start()
    
    # Run consumer on main thread
    instrumented_consumer()
    
    stop_event.set()
    collector.stop()
    time.sleep(0.5)  # Give monitor thread time to finish
    
    return collector, results_count['count']


def plot_comparison(collectors):
    """Generate comparison plots for all operators."""
    fig, axes = plt.subplots(2, 2, figsize=(14, 10))
    
    colors = {'Tumbling Window': 'tab:blue', 'Landmark Window': 'tab:orange', 'Sliding Window': 'tab:green'}
    
    # Stream memory over time
    ax = axes[0, 0]
    for collector in collectors:
        mem_kb = [b / 1024 for b in collector.stats['stream_memory_bytes']]
        ax.plot(collector.stats['timestamps'], mem_kb,
               label=collector.operator_name, color=colors.get(collector.operator_name, 'tab:gray'),
               marker='o', markersize=3, alpha=0.7)
    ax.set_xlabel('Time (s)')
    ax.set_ylabel('Stream Memory (KB)')
    ax.set_title('Stream Buffer Memory Over Time')
    ax.legend()
    ax.grid(True, alpha=0.3)
    
    # Window state (tuples in current window) over time
    ax = axes[0, 1]
    for collector in collectors:
        ax.plot(collector.stats['timestamps'], collector.stats['window_tuples'],
               label=collector.operator_name, color=colors.get(collector.operator_name, 'tab:gray'),
               marker='o', markersize=3, alpha=0.7)
    ax.set_xlabel('Time (s)')
    ax.set_ylabel('Tuples in Window')
    ax.set_title('Window Size Over Time')
    ax.legend()
    ax.grid(True, alpha=0.3)
    
    # Tuple count over time
    ax = axes[1, 0]
    for collector in collectors:
        ax.plot(collector.stats['timestamps'], collector.stats['tuple_count'],
               label=collector.operator_name, color=colors.get(collector.operator_name, 'tab:gray'),
               marker='o', markersize=3, alpha=0.7)
    ax.set_xlabel('Time (s)')
    ax.set_ylabel('Tuples Processed')
    ax.set_title('Throughput Over Time')
    ax.legend()
    ax.grid(True, alpha=0.3)
    
    # Summary stats bar chart — peak window memory
    ax = axes[1, 1]
    summaries = [c.get_summary() for c in collectors]
    operators = [s['operator'] for s in summaries]
    peak_window_mems = [s['peak_window_memory_bytes'] for s in summaries]
    
    x = range(len(operators))
    bars = ax.bar(x, peak_window_mems, color=[colors.get(op, 'tab:gray') for op in operators])
    ax.set_xticks(x)
    ax.set_xticklabels(operators, rotation=15, ha='right')
    ax.set_ylabel('Peak Window Memory (bytes)')
    ax.set_title('Peak Window Memory')
    ax.grid(True, alpha=0.3, axis='y')
    
    for bar in bars:
        height = bar.get_height()
        ax.text(bar.get_x() + bar.get_width()/2., height,
               f'{height:.0f}', ha='center', va='bottom', fontsize=9)
    
    fig.tight_layout()
    plot_path = 'benchmark_comparison.png'
    fig.savefig(plot_path, dpi=100)
    logging.info(f'Comparison plot saved to {plot_path}')
    
    try:
        plt.show()
    except Exception:
        logging.info('Plot display unavailable; open %s manually instead.', plot_path)


def save_results_csv(collectors, filename='benchmark_results.csv'):
    """Save aggregated results to CSV."""
    with open(filename, 'w', newline='') as f:
        writer = csv.writer(f)
        
        # Header
        writer.writerow(['Operator', 'Peak Memory (MB)', 'Avg Memory (MB)', 
                        'Final Memory (MB)', 'Peak Stream Mem (KB)', 'Avg Stream Mem (KB)',
                        'Peak Window Mem (B)', 'Avg Window Mem (B)',
                        'Peak Occupancy', 'Avg CPU (%)', 'Total Tuples',
                        'Duration (s)', 'Throughput (tuples/s)'])
        
        # Data rows
        for collector in collectors:
            summary = collector.get_summary()
            throughput = summary['total_tuples'] / summary['duration_seconds'] if summary['duration_seconds'] > 0 else 0
            peak_occ_str = '; '.join(f"{k}={v}" for k, v in summary['peak_occupancy'].items())
            writer.writerow([
                summary['operator'],
                f"{summary['peak_memory_mb']:.2f}",
                f"{summary['avg_memory_mb']:.2f}",
                f"{summary['final_memory_mb']:.2f}",
                f"{summary['peak_stream_memory_kb']:.2f}",
                f"{summary['avg_stream_memory_kb']:.2f}",
                f"{summary['peak_window_memory_bytes']:.0f}",
                f"{summary['avg_window_memory_bytes']:.0f}",
                peak_occ_str,
                f"{summary['avg_cpu_percent']:.2f}",
                summary['total_tuples'],
                f"{summary['duration_seconds']:.2f}",
                f"{throughput:.2f}",
            ])
    
    logging.info(f'Results saved to {filename}')


def main():
    parser = argparse.ArgumentParser(description='Benchmark window operators on same source')
    parser.add_argument('--source', type=str, default='air_pollution',
                        choices=['air_pollution', 'weather', 'humidity', 'sensor_energy', 'nasdaq'],
                        help='Data source to benchmark (default: air_pollution)')
    parser.add_argument('--window-size', type=int, default=10,
                        help='Tumbling window size in seconds (default: 10)')
    parser.add_argument('--duration', type=int, default=60,
                        help='Benchmark duration in seconds per operator (default: 60)')
    parser.add_argument('--stream-size', type=int, default=20,
                        help='Internal stream buffer size (default: 20)')
    args = parser.parse_args()
    
    logging.basicConfig(level=logging.INFO, 
                       format='[%(levelname)s] (%(threadName)-15s) %(message)s')
    
    sources = {
        'air_pollution': air_pollution_source,
        'weather': weather_source,
        'humidity': humidity_source,
        'sensor_energy': sensor_energy_source,
        'nasdaq': nasdaq_source,
    }
    
    source_func = sources[args.source]
    source_name = f'{args.source.title()} Stream'
    
    logging.info(f'Starting benchmark on {source_name}...')
    logging.info(f'Duration: {args.duration}s per operator')
    
    collectors = []
    
    # Benchmark Tumbling Window
    logging.info('Running Tumbling Window...')
    start = time.time()
    collector_tw, count_tw = benchmark_operator(
        'Tumbling Window',
        None,
        source_func,
        source_name,
        stream_size=args.stream_size,
        window_size=args.window_size,
        duration=args.duration
    )
    collectors.append(collector_tw)
    elapsed_tw = time.time() - start
    logging.info(f'Tumbling Window done: {count_tw} outputs in {elapsed_tw:.2f}s')
    
    time.sleep(2)  # Cool-down period
    
    # Benchmark Landmark Window
    logging.info('Running Landmark Window...')
    start = time.time()
    collector_lw, count_lw = benchmark_operator(
        'Landmark Window',
        None,
        source_func,
        source_name,
        stream_size=args.stream_size,
        duration=args.duration
    )
    collectors.append(collector_lw)
    elapsed_lw = time.time() - start
    logging.info(f'Landmark Window done: {count_lw} outputs in {elapsed_lw:.2f}s')
    
    time.sleep(2)  # Cool-down period
    
    # Benchmark Sliding Window
    logging.info('Running Sliding Window...')
    start = time.time()
    collector_sw, count_sw = benchmark_operator(
        'Sliding Window',
        None,
        source_func,
        source_name,
        stream_size=args.stream_size,
        window_size=args.window_size * 3,
        duration=args.duration
    )
    collectors.append(collector_sw)
    elapsed_sw = time.time() - start
    logging.info(f'Sliding Window done: {count_sw} outputs in {elapsed_sw:.2f}s')
    
    # Save and plot results
    save_results_csv(collectors)
    plot_comparison(collectors)
    
    # Print summary
    logging.info('\n' + '='*60)
    logging.info('BENCHMARK SUMMARY')
    logging.info('='*60)
    for collector in collectors:
        summary = collector.get_summary()
        logging.info(f"\n{summary['operator']}:")
        logging.info(f"  Peak Memory:      {summary['peak_memory_mb']:.2f} MB")
        logging.info(f"  Avg Memory:       {summary['avg_memory_mb']:.2f} MB")
        logging.info(f"  Peak Stream Mem:  {summary['peak_stream_memory_kb']:.2f} KB")
        logging.info(f"  Avg Stream Mem:   {summary['avg_stream_memory_kb']:.2f} KB")
        logging.info(f"  Peak Window Mem:  {summary['peak_window_memory_bytes']:.0f} B")
        logging.info(f"  Avg Window Mem:   {summary['avg_window_memory_bytes']:.0f} B")
        for name, count in summary['peak_occupancy'].items():
            logging.info(f"  Peak Occupancy [{name}]: {count} tuples")
        logging.info(f"  Avg CPU:          {summary['avg_cpu_percent']:.2f} %")
        logging.info(f"  Total Tuples:     {summary['total_tuples']}")
        logging.info(f"  Duration:         {summary['duration_seconds']:.2f} s")
        throughput = summary['total_tuples'] / summary['duration_seconds'] if summary['duration_seconds'] > 0 else 0
        logging.info(f"  Throughput:       {throughput:.2f} tuples/s")
    
    logging.info('Benchmark complete. Exiting.')
    sys.exit(0)


if __name__ == '__main__':
    main()
