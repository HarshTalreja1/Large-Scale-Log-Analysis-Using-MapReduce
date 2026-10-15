"""
mapreduce_log_analyzer.py
--------------------------
Mini Project: Large-Scale Log Analysis Using MapReduce
CO4 : MapReduce

A self-contained, dependency-light MapReduce ENGINE (Map -> Shuffle/Sort ->
Reduce) built with Python's multiprocessing module so the classic phases
of a MapReduce job are genuinely executed in parallel, the same way they
would be spread across nodes in Hadoop.

WHY THIS DESIGN
----------------
A real Hadoop cluster is overkill for a mini project. To still demonstrate
the MapReduce PARADIGM correctly, we implement all four phases explicitly:

    1. INPUT SPLITTING  -> log file is split into N chunks
    2. MAP               -> each chunk is processed in parallel by a
                             mapper process; every log line emits several
                             (key, value) pairs, one per metric we care about
    3. SHUFFLE & SORT     -> intermediate pairs are grouped by key
    4. REDUCE             -> each key's values are aggregated in parallel
                             by reducer processes

The single MapReduce job below computes FOUR analyses at once (they all
share the same Map phase, which is exactly how a real log-analytics
MapReduce job is written):

    - PAGE   : most frequently accessed pages
    - ERROR  : count of each HTTP error status code (4xx/5xx)
    - USER   : requests per client IP (user activity)
    - HOUR   : requests per hour of day (traffic pattern)

Run:
    python3 mapreduce_log_analyzer.py access.log
"""

import sys
import re
import csv
import os
from collections import defaultdict
from multiprocessing import Pool, cpu_count

LOG_PATTERN = re.compile(
    r'(?P<ip>\S+) \S+ \S+ \[(?P<time>[^\]]+)\] '
    r'"(?P<method>\S+) (?P<path>\S+) \S+" '
    r'(?P<status>\d{3}) (?P<size>\S+)'
)

NUM_MAPPERS = max(2, cpu_count())
NUM_REDUCERS = max(2, cpu_count())


# ---------------------------------------------------------------------
# 1. INPUT SPLITTING
# ---------------------------------------------------------------------
def split_input(lines, n_splits):
    """Split the list of log lines into n roughly-equal chunks (input splits)."""
    chunk_size = max(1, len(lines) // n_splits)
    return [lines[i:i + chunk_size] for i in range(0, len(lines), chunk_size)]


# ---------------------------------------------------------------------
# 2. MAP PHASE
# ---------------------------------------------------------------------
def map_function(chunk):
    """
    Mapper: takes a chunk (input split) of raw log lines and emits
    intermediate (key, value) pairs. Runs inside a worker process.
    """
    emitted = []
    for line in chunk:
        m = LOG_PATTERN.search(line)
        if not m:
            continue  # skip malformed lines

        path = m.group("path")
        status = m.group("status")
        ip = m.group("ip")
        time_str = m.group("time")  # e.g. 20/Sep/2026:14:35:02 +0000

        try:
            hour = time_str.split(":")[1]  # HH
        except IndexError:
            hour = "unknown"

        # one map() call -> multiple emitted key/value pairs (fan-out),
        # exactly as encouraged in real MapReduce log-analysis jobs
        emitted.append((f"PAGE::{path}", 1))
        emitted.append((f"USER::{ip}", 1))
        emitted.append((f"HOUR::{hour}", 1))
        if status.startswith("4") or status.startswith("5"):
            emitted.append((f"ERROR::{status}", 1))

    return emitted


# ---------------------------------------------------------------------
# 3. SHUFFLE & SORT PHASE
# ---------------------------------------------------------------------
def shuffle_and_sort(mapped_chunks):
    """
    Groups all intermediate (key, value) pairs coming back from every
    mapper by key. This is the "shuffle" step that a real MapReduce
    framework performs between Map and Reduce.
    """
    grouped = defaultdict(list)
    for chunk_result in mapped_chunks:
        for key, value in chunk_result:
            grouped[key].append(value)
    # sort by key so reducer work is deterministic (like Hadoop's sort)
    return sorted(grouped.items(), key=lambda kv: kv[0])


# ---------------------------------------------------------------------
# 4. REDUCE PHASE
# ---------------------------------------------------------------------
def reduce_function(item):
    """Reducer: aggregates all values for a single key. Runs in a worker."""
    key, values = item
    return key, sum(values)


def run_mapreduce_job(log_lines):
    """Orchestrates the full Map -> Shuffle -> Reduce pipeline."""
    splits = split_input(log_lines, NUM_MAPPERS)
    print(f"[Input Splitting] {len(log_lines)} lines -> {len(splits)} splits")

    with Pool(processes=NUM_MAPPERS) as pool:
        print(f"[Map Phase]      running {len(splits)} mappers in parallel...")
        mapped = pool.map(map_function, splits)

    grouped = shuffle_and_sort(mapped)
    print(f"[Shuffle & Sort] {len(grouped)} unique intermediate keys")

    with Pool(processes=NUM_REDUCERS) as pool:
        print(f"[Reduce Phase]   running reducers in parallel...")
        reduced = pool.map(reduce_function, grouped)

    return dict(reduced)


# ---------------------------------------------------------------------
# Output helpers
# ---------------------------------------------------------------------
def split_results(results):
    """Split the flat {category::key: count} dict into per-category dicts."""
    categories = {"PAGE": {}, "USER": {}, "HOUR": {}, "ERROR": {}}
    for full_key, count in results.items():
        category, _, key = full_key.partition("::")
        if category in categories:
            categories[category][key] = count
    return categories


def write_csv(filename, header, rows):
    with open(filename, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(rows)


def print_top(title, counter, n=10):
    print(f"\n=== {title} (top {n}) ===")
    for key, count in sorted(counter.items(), key=lambda kv: -kv[1])[:n]:
        print(f"  {key:<30} {count}")


def main():
    log_file = sys.argv[1] if len(sys.argv) > 1 else "access.log"
    if not os.path.exists(log_file):
        print(f"Log file not found: {log_file}")
        print("Tip: run `python3 generate_sample_log.py` first to create one.")
        sys.exit(1)

    with open(log_file) as f:
        log_lines = [line.strip() for line in f if line.strip()]

    print(f"Loaded {len(log_lines)} log lines from {log_file}\n")

    results = run_mapreduce_job(log_lines)
    categories = split_results(results)

    # ---- console summary ----
    print_top("Most Frequently Accessed Pages", categories["PAGE"], n=10)
    print_top("Error Occurrences (HTTP status)", categories["ERROR"], n=10)
    print_top("User Activity (by IP)", categories["USER"], n=10)
    print_top("Traffic Pattern (by hour of day)", categories["HOUR"], n=24)

    # ---- write CSV outputs ----
    os.makedirs("output", exist_ok=True)

    write_csv(
        "output/page_frequency.csv", ["page", "hits"],
        sorted(categories["PAGE"].items(), key=lambda kv: -kv[1]),
    )
    write_csv(
        "output/error_occurrences.csv", ["status_code", "count"],
        sorted(categories["ERROR"].items(), key=lambda kv: -kv[1]),
    )
    write_csv(
        "output/user_activity.csv", ["ip_address", "requests"],
        sorted(categories["USER"].items(), key=lambda kv: -kv[1]),
    )
    write_csv(
        "output/hourly_traffic.csv", ["hour", "requests"],
        sorted(categories["HOUR"].items(), key=lambda kv: int(kv[0]) if kv[0].isdigit() else 99),
    )

    print("\nCSV reports written to ./output/")
    print("  - page_frequency.csv")
    print("  - error_occurrences.csv")
    print("  - user_activity.csv")
    print("  - hourly_traffic.csv")


if __name__ == "__main__":
    main()
