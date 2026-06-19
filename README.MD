To run operator, simply run the operator script directly, e.g. `python tumbling_window.py --source air_pollution --window-size 10` to run the tumbling window operator on the air pollution stream with 10 second windows. Each operator script has its own command-line arguments for configuring the data source, window size, and other parameters.

To run benchmarks, here are the scripts:

python3 benchmark_windows.py --source air_pollution --duration 35

python3 benchmark_joins.py --source1 humidity --source2 air_pollution --duration 35