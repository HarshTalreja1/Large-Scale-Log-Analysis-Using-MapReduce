# Large-Scale Log Analysis Using MapReduce


## Problem Statement

Develop a MapReduce-based system for analyzing large volumes of server /
application logs. The system identifies frequently accessed pages, error
occurrences, user activity, and traffic patterns using Map and Reduce
operations.

## How it works

This project implements a real **Map → Shuffle & Sort → Reduce** pipeline
in Python using the `multiprocessing` module, so the classic MapReduce
phases genuinely run in parallel (no Hadoop cluster needed — perfect for a
local mini project, and the code maps 1:1 onto how you'd explain/port it to
Hadoop Streaming).

```
access.log
    │
    ▼
[1] INPUT SPLITTING   → log file split into N chunks
    ▼
[2] MAP (parallel)    → each line emits (key, 1) pairs:
                           PAGE::/products   -> 1
                           USER::10.1.2.3    -> 1
                           HOUR::14          -> 1
                           ERROR::404        -> 1   (only for 4xx/5xx)
    ▼
[3] SHUFFLE & SORT     → group all values by key
    ▼
[4] REDUCE (parallel) → sum values per key
    ▼
output/*.csv + charts.png
```

One Map phase produces four analyses at once, exactly like a real
log-analytics MapReduce job would.

## Files

| File                        | Purpose                                                                                                                                                      |
| --------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `app.py`                    | Flask backend — runs the MapReduce engine live for the dashboard, writes results to `output/`, and serves them for download                                  |
| `dashboard.html`            | The UI — served by `app.py`, or open standalone and upload CSVs manually                                                                                     |
| `generate_sample_log.py`    | Creates a synthetic `access.log` (Apache combined log format). Random by default — run it again for a different dataset, or pass a seed for a repeatable one |
| `mapreduce_log_analyzer.py` | The MapReduce engine + CLI driver — does the actual analysis                                                                                                 |
| `visualize_results.py`      | Turns the CSV outputs into a static chart image for your report                                                                                              |
| `access.log`                | A generated sample log (regenerate anytime with `generate_sample_log.py`)                                                                                    |
| `output/`                   | CSV results + `charts.png` — written by both the CLI script and the dashboard                                                                                |

## How to run

### Option A — Connected dashboard (recommended): backend + live results

```bash
pip install flask
python3 app.py
```

Then open **http://127.0.0.1:5000** in your browser (not the file directly —
the dashboard needs the backend to actually run anything). The "Backend:
connected ✓" badge at the top confirms the server is reachable.

- **"Upload & Analyze Log File"** — pick any log file; it's sent to the
  backend, analyzed live with the real MapReduce engine, and the charts
  update with the result.
- **"Run on sample"** — runs the same engine on the bundled `access.log`.
- Both of these also write fresh CSVs to `output/` on the server, and enable
  the **"Download CSVs (.zip)"** button so you can grab
  `page_frequency.csv`, `error_occurrences.csv`, `user_activity.csv`, and
  `hourly_traffic.csv` straight from the browser.
- **"Upload CSVs"** — a separate, backend-independent path: pick 4
  already-generated CSVs (e.g. from a previous `output/` folder) and the
  dashboard loads them directly in-browser. Useful for viewing results
  offline, but this path does **not** talk to the server or update
  `output/` — it's for viewing existing CSVs, not analyzing new logs.
- **"Clear results"** — resets the dashboard to its empty state.

The dashboard starts empty (no fake/placeholder numbers) until you actually
run one of the above.

### Option B — Command line, no backend

```bash
# 1. Generate a sample log file (skip this if you have a real one)
python3 generate_sample_log.py
#    add a number for a repeatable log instead of a random one:
python3 generate_sample_log.py 42

# 2. Run the MapReduce analysis
python3 mapreduce_log_analyzer.py access.log

# 3. (optional) Generate a static chart image for your report
python3 visualize_results.py
```

This writes `output/*.csv` and, with step 3, `output/charts.png`. You can
then open `dashboard.html` directly (no server) and use its **"Upload CSVs"**
section to load those files.

To analyze **your own** log file with either option, just point it at your
file instead of `access.log`. It expects the standard Apache/Nginx combined
log format:

```
IP - - [DD/Mon/YYYY:HH:MM:SS +0000] "METHOD /path HTTP/1.1" STATUS SIZE "-" "UserAgent"
```

## Verifying it's not hardcoded

`generate_sample_log.py` used to pin its random seed, so every "new" log
was actually identical — that's fixed now (random by default). To see the
system respond to genuinely different input:

1. Run `python3 generate_sample_log.py` twice — each run overwrites
   `access.log` with different random traffic.
2. Upload each version through **"Upload & Analyze Log File"** and watch
   the numbers change.
3. Or hand-write a single log line with a page name you invent (e.g.
   `/definitely-not-a-real-page`) and upload just that — it'll show up
   in the "Most Accessed Pages" chart with exactly the count you gave it.

## API endpoints (`app.py`)

| Endpoint               | Method | Purpose                                                                       |
| ---------------------- | ------ | ----------------------------------------------------------------------------- |
| `/`                    | GET    | Serves `dashboard.html`                                                       |
| `/api/health`          | GET    | Health check used by the dashboard's status badge                             |
| `/api/analyze/sample`  | POST   | Runs the MapReduce engine on the bundled `access.log`                         |
| `/api/analyze/upload`  | POST   | Runs the MapReduce engine on an uploaded log file (multipart field `logfile`) |
| `/api/download/<name>` | GET    | Downloads one CSV from `output/` (name must be one of the 4 known filenames)  |
| `/api/download/all`    | GET    | Downloads all 4 CSVs bundled as `log_analysis_results.zip`                    |

`/api/download/*` return a 404 with a JSON error until at least one
analysis has been run (via `/api/analyze/sample` or `/api/analyze/upload`),
since there's nothing in `output/` yet.

## Outputs produced

- **`output/page_frequency.csv`** — every page and how many times it was hit (most-accessed pages)
- **`output/error_occurrences.csv`** — count of each HTTP error status code (404, 500, 403, ...)
- **`output/user_activity.csv`** — requests per client IP (busiest users)
- **`output/hourly_traffic.csv`** — requests per hour of day (traffic pattern / peak hours)
- **`output/charts.png`** — visual summary of all four analyses (via `visualize_results.py`)

## Design notes for your report

- **Mapper:** `map_function()` — parses one log line with a regex and emits
  multiple `(key, value)` pairs (fan-out), one per metric.
- **Shuffle:** `shuffle_and_sort()` — groups intermediate pairs by key
  (the step a real MapReduce framework does automatically between nodes).
- **Reducer:** `reduce_function()` — sums the values for a key (a classic
  word-count-style reducer, applied to 4 different key families at once).
- **Parallelism:** `multiprocessing.Pool` runs the mappers concurrently
  and the reducers concurrently, mirroring how work is distributed across
  nodes in Hadoop/Spark.
- This design **scales**: for a genuinely huge log file you would swap
  the in-memory `Pool.map` calls for Hadoop Streaming (`mapreduce_log_analyzer.py`'s
  `map_function`/`reduce_function` could be dropped almost unchanged into
  `mapper.py` / `reducer.py` scripts for `hadoop-streaming.jar`), or for
  PySpark's `rdd.flatMap()` / `rdd.reduceByKey()`.
