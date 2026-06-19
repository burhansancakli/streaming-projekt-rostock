#!/usr/bin/env python3

import threading
import time
import argparse
import logging
import csv
import sys
import sys
from collections import deque

import psutil
import matplotlib.pyplot as plt

from stream import stream
from air_pollution_source import air_pollution_source
from weather_source import weather_source
from humidity_source import humidity_source
from sensor_energy_source import sensor_energy_source
from fuzzy_merge_join import fuzzy_merge_join, sink as fmj_sink
from bufferless_fuzzy_merge_join import bufferless_fuzzy_merge_join, sink as bfmj_sink
from minimal_delta_joining import minimal_delta_joining, sink as mdj_sink


class StatsCollector:
    """Collects memory, CPU, and throughput statistics during execution."""
    
    TUPLE_OVERHEAD = 64  # approximate bytes per (timestamp, value) tuple
    
    def __init__(self, operator_name, process, streams=None):
        self.operator_name = operator_name
        self.process = process
        self.streams = streams or []
        self.stats = {
            'timestamps': [],
            'memory_mb': [],
            'stream_memory_bytes': [],
            'stream_occupancy': [],
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
        
        peak_occupancy = {}
        for occ in self.stats['stream_occupancy']:
            for name, count in occ.items():
                peak_occupancy[name] = max(peak_occupancy.get(name, 0), count)
        
        return {
            'operator': self.operator_name,
            'peak_memory_mb': max(self.stats['memory_mb']),
            'avg_memory_mb': sum(self.stats['memory_mb']) / len(self.stats['memory_mb']),
            'final_memory_mb': self.stats['memory_mb'][-1],
            'peak_stream_memory_bytes': peak_stream_mem,
            'avg_stream_memory_bytes': avg_stream_mem,
            'peak_stream_memory_kb': peak_stream_mem / 1024,
            'avg_stream_memory_kb': avg_stream_mem / 1024,
            'peak_occupancy': peak_occupancy,
            'avg_cpu_percent': sum(self.stats['cpu_percent']) / len(self.stats['cpu_percent']) if self.stats['cpu_percent'] else 0,
            'total_tuples': self.tuple_count,
            'duration_seconds': time.time() - self.start_time,
        }


def benchmark_join_operator(operator_name, join_func, source1_func, source2_func,
                           source1_name, source2_name, duration=60,
                           s1_stream_size=10, s2_stream_size=10):
    """
    Run a single join operator for a fixed duration with stats collection.
    Returns StatsCollector with statistics.
    """
    process = psutil.Process()

    
    stream1 = stream(source1_name, s1_stream_size)
    stream2 = stream(source2_name, s2_stream_size)
    result_stream = stream("Result Stream", 10)
    
    collector = StatsCollector(operator_name, process, streams=[stream1, stream2, result_stream])
    results = []
    stop_event = threading.Event()
    
    # Wrap result_stream.put_force to check stop_event
    original_put_force = result_stream.put_force
    def guarded_put_force(t):
        if stop_event.is_set():
            raise RuntimeError("Benchmark stopped")
        return original_put_force(t)
    result_stream.put_force = guarded_put_force
    
    # Wrap result_stream.get to check stop_event periodically
    def guarded_get():
        while not stop_event.is_set():
            result_stream._mutex.acquire()
            if result_stream._isempty():
                result_stream._mutex.wait(timeout=0.5)
                if result_stream._isempty():
                    result_stream._mutex.release()
                    continue
            t = result_stream._dequeue()
            result_stream._mutex.notify()
            result_stream._mutex.release()
            return t
        raise RuntimeError("Benchmark stopped")
    result_stream.get = guarded_get
    
    def instrumented_sink():
        """Sink that counts results until stop_event is set."""
        try:
            while not stop_event.is_set():
                result = result_stream.get()
                results.append(result)
                collector.increment_tuples()
        except RuntimeError as e:
            if "Benchmark stopped" in str(e):
                logging.debug(f"Sink stopped: {e}")
            else:
                logging.debug(f"Sink exception: {e}")
        except Exception as e:
            logging.debug(f"Sink exception: {e}")
    
    def instrumented_join():
        """Run the join operator with graceful stop on signal."""
        try:
            if operator_name == 'Fuzzy Merge Join':
                fuzzy_merge_join(stream1, stream2, result_stream)
            elif operator_name == 'Bufferless Fuzzy Merge Join':
                bufferless_fuzzy_merge_join(stream1, stream2, result_stream)
            elif operator_name == 'Minimal Delta Join':
                minimal_delta_joining(stream1, stream2, window_size=10)
        except RuntimeError as e:
            if "Benchmark stopped" in str(e):
                logging.debug(f"Join operator stopped: {e}")
            else:
                logging.debug(f"Join exception: {e}")
        except Exception as e:
            logging.debug(f"Join exception: {e}")
    
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
    
    # Start source threads
    source1_thread = threading.Thread(name='source1', target=source1_func,
                                     args=(stream1,), daemon=True)
    source2_thread = threading.Thread(name='source2', target=source2_func,
                                     args=(stream2,), daemon=True)
    
    # Start join and sink threads
    join_thread = threading.Thread(name='join_operator', target=instrumented_join, daemon=True)
    sink_thread = threading.Thread(name='sink', target=instrumented_sink, daemon=True)
    
    source1_thread.start()
    source2_thread.start()
    join_thread.start()
    sink_thread.start()
    
    # Wait for sink to finish (timer sets stop_event, which stops sink)
    sink_thread.join(timeout=duration + 5)
    stop_event.set()
    
    time.sleep(0.5)
    
    collector.stop()
    time.sleep(0.5)
    
    return collector, len(results)


def plot_comparison(collectors):
    """Generate comparison plots for all join operators."""
    fig, axes = plt.subplots(2, 2, figsize=(14, 10))
    
    colors = {
        'Fuzzy Merge Join': 'tab:blue',
        'Bufferless Fuzzy Merge Join': 'tab:orange',
        'Minimal Delta Join': 'tab:green'
    }
    
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
    
    # CPU over time
    ax = axes[0, 1]
    for collector in collectors:
        ax.plot(collector.stats['timestamps'], collector.stats['cpu_percent'],
               label=collector.operator_name, color=colors.get(collector.operator_name, 'tab:gray'),
               marker='o', markersize=3, alpha=0.7)
    ax.set_xlabel('Time (s)')
    ax.set_ylabel('CPU (%)')
    ax.set_title('CPU Usage Over Time')
    ax.legend()
    ax.grid(True, alpha=0.3)
    
    # Tuple count over time
    ax = axes[1, 0]
    for collector in collectors:
        ax.plot(collector.stats['timestamps'], collector.stats['tuple_count'],
               label=collector.operator_name, color=colors.get(collector.operator_name, 'tab:gray'),
               marker='o', markersize=3, alpha=0.7)
    ax.set_xlabel('Time (s)')
    ax.set_ylabel('Output Tuples')
    ax.set_title('Throughput Over Time')
    ax.legend()
    ax.grid(True, alpha=0.3)
    
    # Summary stats bar chart — peak stream memory
    ax = axes[1, 1]
    summaries = [c.get_summary() for c in collectors]
    operators = [s['operator'] for s in summaries]
    peak_stream_mems = [s['peak_stream_memory_kb'] for s in summaries]
    
    x = range(len(operators))
    bars = ax.bar(x, peak_stream_mems, color=[colors.get(op, 'tab:gray') for op in operators])
    ax.set_xticks(x)
    ax.set_xticklabels(operators, rotation=15, ha='right')
    ax.set_ylabel('Peak Stream Memory (KB)')
    ax.set_title('Peak Stream Buffer Memory')
    ax.grid(True, alpha=0.3, axis='y')
    
    for bar in bars:
        height = bar.get_height()
        ax.text(bar.get_x() + bar.get_width()/2., height,
               f'{height:.2f}', ha='center', va='bottom', fontsize=9)
    
    fig.tight_layout()
    plot_path = 'benchmark_joins_comparison.png'
    fig.savefig(plot_path, dpi=100)
    logging.info(f'Comparison plot saved to {plot_path}')
    
    try:
        plt.show()
    except Exception:
        logging.info('Plot display unavailable; open %s manually instead.', plot_path)


def save_results_csv(collectors, filename='benchmark_joins_results.csv'):
    """Save aggregated results to CSV."""
    with open(filename, 'w', newline='') as f:
        writer = csv.writer(f)
        
        # Header
        writer.writerow(['Operator', 'Peak Memory (MB)', 'Avg Memory (MB)',
                        'Final Memory (MB)', 'Peak Stream Mem (KB)', 'Avg Stream Mem (KB)',
                        'Peak Occupancy', 'Avg CPU (%)', 'Total Output Tuples',
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
                peak_occ_str,
                f"{summary['avg_cpu_percent']:.2f}",
                summary['total_tuples'],
                f"{summary['duration_seconds']:.2f}",
                f"{throughput:.2f}",
            ])
    
    logging.info(f'Results saved to {filename}')


def main():
    parser = argparse.ArgumentParser(description='Benchmark join operators on same sources')
    parser.add_argument('--source1', type=str, default='weather',
                        choices=['air_pollution', 'weather', 'humidity', 'sensor_energy'],
                        help='First stream source (default: weather)')
    parser.add_argument('--source2', type=str, default='humidity',
                        choices=['air_pollution', 'weather', 'humidity', 'sensor_energy'],
                        help='Second stream source (default: humidity)')
    parser.add_argument('--duration', type=int, default=60,
                        help='Benchmark duration in seconds per operator (default: 60)')
    parser.add_argument('--stream-size', type=int, default=10,
                        help='Internal stream buffer size (default: 10)')
    args = parser.parse_args()
    
    logging.basicConfig(level=logging.INFO,
                       format='[%(levelname)s] (%(threadName)-15s) %(message)s')
    
    sources = {
        'air_pollution': air_pollution_source,
        'weather': weather_source,
        'humidity': humidity_source,
        'sensor_energy': sensor_energy_source,
    }
    
    source1_func = sources[args.source1]
    source2_func = sources[args.source2]
    source1_name = f'{args.source1.title()} Stream'
    source2_name = f'{args.source2.title()} Stream'
    
    logging.info(f'Starting join benchmark: {args.source1} x {args.source2}')
    logging.info(f'Duration: {args.duration}s per operator')
    
    collectors = []
    
    # Benchmark Fuzzy Merge Join
    logging.info('Running Fuzzy Merge Join...')
    start = time.time()
    collector_fmj, count_fmj = benchmark_join_operator(
        'Fuzzy Merge Join',
        fuzzy_merge_join,
        source1_func,
        source2_func,
        source1_name,
        source2_name,
        duration=args.duration,
        s1_stream_size=args.stream_size,
        s2_stream_size=args.stream_size
    )
    collectors.append(collector_fmj)
    elapsed_fmj = time.time() - start
    logging.info(f'Fuzzy Merge Join done: {count_fmj} outputs in {elapsed_fmj:.2f}s')
    
    time.sleep(2)  # Cool-down
    
    # Benchmark Bufferless Fuzzy Merge Join
    logging.info('Running Bufferless Fuzzy Merge Join...')
    start = time.time()
    collector_bfmj, count_bfmj = benchmark_join_operator(
        'Bufferless Fuzzy Merge Join',
        bufferless_fuzzy_merge_join,
        source1_func,
        source2_func,
        source1_name,
        source2_name,
        duration=args.duration,
        s1_stream_size=args.stream_size,
        s2_stream_size=args.stream_size
    )
    collectors.append(collector_bfmj)
    elapsed_bfmj = time.time() - start
    logging.info(f'Bufferless Fuzzy Merge Join done: {count_bfmj} outputs in {elapsed_bfmj:.2f}s')
    
    time.sleep(2)  # Cool-down
    
    # Benchmark Minimal Delta Join
    logging.info('Running Minimal Delta Join...')
    start = time.time()
    collector_mdj, count_mdj = benchmark_join_operator(
        'Minimal Delta Join',
        minimal_delta_joining,
        source1_func,
        source2_func,
        source1_name,
        source2_name,
        duration=args.duration,
        s1_stream_size=args.stream_size,
        s2_stream_size=args.stream_size
    )
    collectors.append(collector_mdj)
    elapsed_mdj = time.time() - start
    logging.info(f'Minimal Delta Join done: {count_mdj} outputs in {elapsed_mdj:.2f}s')
    
    # Save and plot results
    save_results_csv(collectors)
    plot_comparison(collectors)
    
    # Print summary
    logging.info('\n' + '='*70)
    logging.info('JOIN BENCHMARK SUMMARY')
    logging.info('='*70)
    for collector in collectors:
        summary = collector.get_summary()
        logging.info(f"\n{summary['operator']}:")
        logging.info(f"  Peak Memory:      {summary['peak_memory_mb']:.2f} MB")
        logging.info(f"  Avg Memory:       {summary['avg_memory_mb']:.2f} MB")
        logging.info(f"  Peak Stream Mem:  {summary['peak_stream_memory_kb']:.2f} KB")
        logging.info(f"  Avg Stream Mem:   {summary['avg_stream_memory_kb']:.2f} KB")
        for name, count in summary['peak_occupancy'].items():
            logging.info(f"  Peak Occupancy [{name}]: {count} tuples")
        logging.info(f"  Avg CPU:          {summary['avg_cpu_percent']:.2f} %")
        logging.info(f"  Total Outputs:    {summary['total_tuples']}")
        logging.info(f"  Duration:         {summary['duration_seconds']:.2f} s")
        throughput = summary['total_tuples'] / summary['duration_seconds'] if summary['duration_seconds'] > 0 else 0
        logging.info(f"  Throughput:       {throughput:.2f} tuples/s")
    
    logging.info('Benchmark complete. Exiting.')
    sys.exit(0)


if __name__ == '__main__':
    main()
