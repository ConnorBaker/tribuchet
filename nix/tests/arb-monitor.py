# Every 20 ms: each build cgroup's freezer state, runnable and total
# threads and frozen_usec, as one JSON line, until the cgroups are gone or
# SECONDS have passed. Arguments: SECONDS NAME=CGROUP...
import json
import os
import sys
import time

names = dict(a.split("=", 1) for a in sys.argv[2:])


def read(path):
    with open(path) as f:
        return f.read()


def threads(d):
    n, total = 0, 0
    for root, _, _ in os.walk(d):
        try:
            tids = read(os.path.join(root, "cgroup.threads")).split()
        except OSError:
            continue
        total += len(tids)
        for t in tids:
            try:
                if read(f"/proc/{t}/stat").rsplit(") ", 1)[1][0] == "R":
                    n += 1
            except (OSError, IndexError):
                pass
    return n, total


end = time.monotonic() + float(sys.argv[1])
while time.monotonic() < end:
    row = {"t": time.monotonic()}
    for k, d in names.items():
        try:
            ev = read(os.path.join(d, "cgroup.events"))
            stat = read(os.path.join(d, "cgroup.stat.local")).split()
            us = int(stat[stat.index("frozen_usec") + 1])
            r, n = threads(d)
            row[k] = {"frozen": "frozen 1" in ev, "R": r, "n": n, "us": us}
        except (OSError, ValueError, IndexError):
            row[k] = None
    print(json.dumps(row), flush=True)
    if all(row[k] is None for k in names):
        break
    time.sleep(0.02)
