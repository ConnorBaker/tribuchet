# PHASE;PHASE..., each PROCS:MIB:CPU[:EXTRA@AT]: PROCS processes at once,
# each touching MIB MiB of random bytes (zswap cannot compress them), then
# spinning CPU seconds of its own CPU time; process 0 touches EXTRA MiB
# more at AT CPU-s. A process that dies fails the build.
import os
import sys
import time


def touch(mb):
    b = bytearray(mb << 20)
    v = memoryview(b)
    for off in range(0, len(b), 1 << 24):
        n = min(1 << 24, len(b) - off)
        v[off : off + n] = os.urandom(n)
    return b


def child(mb, cpu, extra, at):
    held = [touch(mb)]
    t0 = time.process_time()
    while time.process_time() - t0 < cpu:
        if extra and time.process_time() - t0 >= at:
            held.append(touch(extra))
            extra = 0
    os._exit(0)


for phase in sys.argv[1].split(";"):
    f = phase.split(":")
    procs, mb, cpu = int(f[0]), int(f[1]), float(f[2])
    extra, at = (0, 0.0)
    if len(f) > 3:
        extra, at = int(f[3].split("@")[0]), float(f[3].split("@")[1])
    pids = []
    for i in range(procs):
        pid = os.fork()
        if pid == 0:
            child(mb, cpu, extra if i == 0 else 0, at)
        pids.append(pid)
    if any(os.waitpid(p, 0)[1] for p in pids):
        sys.exit("a process of the build died")
