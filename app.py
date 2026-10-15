"""
app.py
-------
Small Flask backend that connects the dashboard UI directly to the real
MapReduce engine in mapreduce_log_analyzer.py — no manual "run script,
export CSV, upload CSV" steps needed anymore.

It imports and reuses the SAME map_function / shuffle_and_sort /
reduce_function / run_mapreduce_job used by the command-line analyzer,
so the logic is not duplicated — this is just a thin API wrapper around it.

Run:
    pip install flask
    python3 app.py
Then open:
    http://127.0.0.1:5000
"""

import io
import os
import zipfile
from flask import Flask, request, jsonify, send_from_directory, send_file, abort

from mapreduce_log_analyzer import run_mapreduce_job, split_results, write_csv

app = Flask(__name__, static_folder=None)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SAMPLE_LOG = os.path.join(BASE_DIR, "access.log")
OUTPUT_DIR = os.path.join(BASE_DIR, "output")

# Whitelist of files servable via /api/download/<name> — never build a path
# from the request directly, so a filename like "../../app.py" is rejected.
CSV_FILENAMES = [
    "page_frequency.csv",
    "error_occurrences.csv",
    "user_activity.csv",
    "hourly_traffic.csv",
]


def write_output_csvs(categories):
    """Write the same 4 CSVs the CLI script produces, so a run triggered
    from the dashboard also updates output/ on disk (e.g. for the
    'Upload CSVs' offline path, or for inspecting results outside the UI)."""
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    write_csv(
        os.path.join(OUTPUT_DIR, "page_frequency.csv"), ["page", "hits"],
        sorted(categories["PAGE"].items(), key=lambda kv: -kv[1]),
    )
    write_csv(
        os.path.join(OUTPUT_DIR, "error_occurrences.csv"), ["status_code", "count"],
        sorted(categories["ERROR"].items(), key=lambda kv: -kv[1]),
    )
    write_csv(
        os.path.join(OUTPUT_DIR, "user_activity.csv"), ["ip_address", "requests"],
        sorted(categories["USER"].items(), key=lambda kv: -kv[1]),
    )
    write_csv(
        os.path.join(OUTPUT_DIR, "hourly_traffic.csv"), ["hour", "requests"],
        sorted(categories["HOUR"].items(), key=lambda kv: int(kv[0]) if kv[0].isdigit() else 99),
    )


def categories_to_json(categories):
    """Turn {'PAGE': {...}, 'ERROR': {...}, ...} into the sorted list-of-pairs
    format the dashboard's JS already expects (same shape it uses for
    parsed CSVs)."""
    def sorted_desc(d):
        return sorted(d.items(), key=lambda kv: -kv[1])

    def sorted_by_hour(d):
        return sorted(d.items(), key=lambda kv: int(kv[0]) if kv[0].isdigit() else 99)

    return {
        "page": sorted_desc(categories["PAGE"]),
        "error": sorted_desc(categories["ERROR"]),
        "user": sorted_desc(categories["USER"]),
        "hourly": sorted_by_hour(categories["HOUR"]),
    }


@app.route("/")
def index():
    return send_from_directory(BASE_DIR, "dashboard.html")


@app.route("/api/health")
def health():
    """Used by the dashboard's 'Backend: connected' status badge."""
    return jsonify({"status": "ok"})


@app.route("/api/analyze/sample", methods=["POST"])
def analyze_sample():
    """Run the MapReduce engine on the bundled access.log."""
    if not os.path.exists(SAMPLE_LOG):
        return jsonify({"error": "access.log not found. Run generate_sample_log.py first."}), 404

    with open(SAMPLE_LOG) as f:
        lines = [line.strip() for line in f if line.strip()]

    results = run_mapreduce_job(lines)
    categories = split_results(results)
    write_output_csvs(categories)
    return jsonify(categories_to_json(categories))


@app.route("/api/analyze/upload", methods=["POST"])
def analyze_upload():
    """Run the MapReduce engine on a log file uploaded from the browser."""
    if "logfile" not in request.files:
        return jsonify({"error": "No file uploaded (expected field 'logfile')"}), 400

    file = request.files["logfile"]
    raw = file.read().decode("utf-8", errors="ignore")
    lines = [line.strip() for line in raw.splitlines() if line.strip()]

    if not lines:
        return jsonify({"error": "Uploaded file is empty"}), 400

    results = run_mapreduce_job(lines)
    categories = split_results(results)
    write_output_csvs(categories)
    return jsonify(categories_to_json(categories))


@app.route("/api/download/<name>")
def download_csv(name):
    """Download a single generated CSV from output/. Only files in the
    fixed whitelist can be served — request data is never used to build
    the filesystem path."""
    if name not in CSV_FILENAMES:
        abort(404)
    path = os.path.join(OUTPUT_DIR, name)
    if not os.path.exists(path):
        return jsonify({"error": f"{name} not found. Run an analysis first."}), 404
    return send_from_directory(OUTPUT_DIR, name, as_attachment=True)


@app.route("/api/download/all")
def download_all_csvs():
    """Bundle all 4 generated CSVs into a single ZIP for download."""
    missing = [n for n in CSV_FILENAMES if not os.path.exists(os.path.join(OUTPUT_DIR, n))]
    if missing:
        return jsonify({"error": "No results yet. Run an analysis first."}), 404

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for name in CSV_FILENAMES:
            zf.write(os.path.join(OUTPUT_DIR, name), arcname=name)
    buf.seek(0)
    return send_file(
        buf,
        mimetype="application/zip",
        as_attachment=True,
        download_name="log_analysis_results.zip",
    )


if __name__ == "__main__":
    print("Log Analysis dashboard running at: http://127.0.0.1:5000")
    app.run(host="127.0.0.1", port=5000, debug=True)