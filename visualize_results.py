"""
visualize_results.py
---------------------
Reads the CSV outputs produced by mapreduce_log_analyzer.py and draws a
few simple charts for the project report / viva. Run this AFTER the
analyzer has produced ./output/*.csv

Run:  python3 visualize_results.py
Output: output/charts.png
"""

import csv
import matplotlib.pyplot as plt


def read_csv(path, top_n=None):
    with open(path) as f:
        rows = list(csv.reader(f))
    header, data = rows[0], rows[1:]
    if top_n:
        data = data[:top_n]
    return header, data


def main():
    fig, axes = plt.subplots(2, 2, figsize=(13, 9))
    fig.suptitle("Large-Scale Log Analysis Using MapReduce", fontsize=14, fontweight="bold")

    # 1. Top pages
    _, rows = read_csv("output/page_frequency.csv", top_n=10)
    pages = [r[0] for r in rows][::-1]
    hits = [int(r[1]) for r in rows][::-1]
    axes[0, 0].barh(pages, hits, color="#4C72B0")
    axes[0, 0].set_title("Most Frequently Accessed Pages")
    axes[0, 0].set_xlabel("Hits")

    # 2. Error occurrences
    _, rows = read_csv("output/error_occurrences.csv")
    codes = [r[0] for r in rows]
    counts = [int(r[1]) for r in rows]
    axes[0, 1].bar(codes, counts, color="#C44E52")
    axes[0, 1].set_title("Error Occurrences by HTTP Status")
    axes[0, 1].set_xlabel("Status Code")
    axes[0, 1].set_ylabel("Count")

    # 3. Top active users
    _, rows = read_csv("output/user_activity.csv", top_n=10)
    ips = [r[0] for r in rows][::-1]
    reqs = [int(r[1]) for r in rows][::-1]
    axes[1, 0].barh(ips, reqs, color="#55A868")
    axes[1, 0].set_title("Top 10 Most Active Users (by IP)")
    axes[1, 0].set_xlabel("Requests")

    # 4. Hourly traffic pattern
    _, rows = read_csv("output/hourly_traffic.csv")
    hours = [r[0] for r in rows]
    reqs = [int(r[1]) for r in rows]
    axes[1, 1].plot(hours, reqs, marker="o", color="#8172B2")
    axes[1, 1].set_title("Traffic Pattern by Hour of Day")
    axes[1, 1].set_xlabel("Hour (24h)")
    axes[1, 1].set_ylabel("Requests")
    axes[1, 1].tick_params(axis="x", rotation=45)

    plt.tight_layout(rect=[0, 0, 1, 0.96])
    plt.savefig("output/charts.png", dpi=150)
    print("Saved output/charts.png")


if __name__ == "__main__":
    main()
