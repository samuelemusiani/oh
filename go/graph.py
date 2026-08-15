#!/usr/bin/env python3
import sys
import os
import numpy as np
import matplotlib.pyplot as plt

BIN_SIZE = 0.10  # seconds, for v1 only

input_path = sys.argv[1]
out_dir = os.path.dirname(os.path.abspath(input_path))
out_path = os.path.join(out_dir, "rate.png")

t_min = t_max = None
with open(input_path) as f:
    for line in f:
        ts, _, _ = line.split()
        ts = float(ts) / 1e9
        if t_min is None or ts < t_min:
            t_min = ts
        if t_max is None or ts > t_max:
            t_max = ts

duration = t_max - t_min
nbins = int(duration / BIN_SIZE) + 1
if nbins > 2_000_000:
    raise SystemExit(f"nbins={nbins} too large, check timestamp units or increase BIN_SIZE")

bins_v1 = np.zeros(nbins, dtype=np.float64)
v2_times = []
v2_values = []

with open(input_path) as f:
    for line in f:
        ts, v1, v2 = line.split()
        ts = float(ts) / 1e9
        idx = int((ts - t_min) / BIN_SIZE)
        if idx >= nbins:
            idx = nbins - 1
        bins_v1[idx] += float(v1)

        v2_times.append(ts - t_min)
        v2_values.append(float(v2) * 1000/8)

times_v1 = np.arange(nbins) * BIN_SIZE
rate_v1 = bins_v1 / BIN_SIZE

plt.figure(figsize=(10, 5))
plt.plot(times_v1, rate_v1, label="sending rate (bytes/s)")
plt.plot(v2_times, v2_values, label="rate limit", drawstyle="steps-post")

THRESHOLD = 10 * 1024 * 1024 / 8  # 10 Mbit/s in bytes/s
plt.axhline(THRESHOLD, color="red", linestyle="--", linewidth=1, label="10 Mbit/s")

plt.xlabel("Time (s)")
plt.ylabel("bytes/s")
plt.legend()
plt.grid(True)
plt.tight_layout()
plt.savefig(out_path, dpi=150)
plt.show()
