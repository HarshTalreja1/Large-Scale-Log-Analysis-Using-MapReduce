"""
generate_sample_log.py
-----------------------
Creates a synthetic web-server access log (Apache/Nginx "combined" style)
so the MapReduce analyzer has real data to chew on.

Run:  python3 generate_sample_log.py
Output: access.log  (in the same folder)
"""

import random
import sys
from datetime import datetime, timedelta

# No fixed seed by default: every run generates a genuinely different log,
# so re-running this script (or uploading a fresh log) actually produces
# different numbers in the dashboard. Pass a number on the command line
# (e.g. `python3 generate_sample_log.py 42`) if you ever want a repeatable
# log for debugging.
if len(sys.argv) > 1:
    random.seed(int(sys.argv[1]))

PAGES = [
    "/index.html", "/home", "/products", "/products/laptop",
    "/products/phone", "/cart", "/checkout", "/login", "/logout",
    "/about", "/contact", "/api/users", "/api/orders", "/search",
    "/images/logo.png", "/css/style.css", "/favicon.ico",
]

# weight some pages as more popular than others
PAGE_WEIGHTS = [10, 8, 15, 12, 9, 6, 4, 7, 3, 2, 2, 5, 4, 6, 3, 3, 2]

METHODS = ["GET", "GET", "GET", "GET", "POST", "PUT", "DELETE"]

STATUS_CODES = [200, 200, 200, 200, 200, 301, 302, 404, 404, 500, 403]

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15)",
    "Mozilla/5.0 (X11; Linux x86_64)",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X)",
]

NUM_UNIQUE_USERS = 60
NUM_LOG_LINES = 4000


def random_ip_pool(n):
    return [f"10.{random.randint(0,255)}.{random.randint(0,255)}.{random.randint(1,254)}"
            for _ in range(n)]


def main():
    ip_pool = random_ip_pool(NUM_UNIQUE_USERS)
    start_time = datetime(2026, 9, 20, 0, 0, 0)

    lines = []
    for _ in range(NUM_LOG_LINES):
        ip = random.choice(ip_pool)
        # spread requests across a 24-hour window, busier in "daytime" hours
        hour_weights = [1,1,1,1,1,2,3,5,7,9,10,10,9,8,9,10,10,9,8,6,4,3,2,1]
        hour = random.choices(range(24), weights=hour_weights, k=1)[0]
        minute = random.randint(0, 59)
        second = random.randint(0, 59)
        ts = start_time.replace(hour=hour, minute=minute, second=second)
        ts += timedelta(days=random.choice([0, 0, 0, 1]))  # mostly day 0, some day 1

        method = random.choice(METHODS)
        page = random.choices(PAGES, weights=PAGE_WEIGHTS, k=1)[0]
        status = random.choice(STATUS_CODES)
        size = random.randint(200, 50000)
        ua = random.choice(USER_AGENTS)

        line = (
            f'{ip} - - [{ts.strftime("%d/%b/%Y:%H:%M:%S +0000")}] '
            f'"{method} {page} HTTP/1.1" {status} {size} "-" "{ua}"'
        )
        lines.append(line)

    # keep the file roughly time-ordered, like a real log
    lines.sort(key=lambda l: l.split("[")[1].split("]")[0])

    with open("access.log", "w") as f:
        f.write("\n".join(lines) + "\n")

    print(f"Generated {len(lines)} log lines -> access.log")


if __name__ == "__main__":
    main()