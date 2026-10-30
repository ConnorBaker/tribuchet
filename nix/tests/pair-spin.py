# 4 processes, each spinning until it has run SECS by the wall clock less
# its gaps of 0.5 s or more (a freeze), whatever share of a CPU it gets: a
# fixed running time on any builder. The first prints a line every 2 s of
# it.
import os
import sys
import time


def spin(secs, say):
    ran, last, said = 0.0, time.monotonic(), 0
    while ran < secs:
        now = time.monotonic()
        if now - last < 0.5:
            ran += now - last
        last = now
        if say and ran >= said + 2:
            said += 2
            print(f"ran {said} s", flush=True)
    os._exit(0)


pids = []
for i in range(4):
    pid = os.fork()
    if pid == 0:
        spin(float(sys.argv[1]), i == 0)
    pids.append(pid)
if any(os.waitpid(p, 0)[1] for p in pids):
    sys.exit("a process of the build died")
