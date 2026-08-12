#!/usr/bin/env python3

import sys
import matplotlib.pyplot as plt

times = []
values = []

with open(sys.argv[1]) as f:
    t0 = None
    for line in f:
        ts, val = line.split()
        ts = int(ts)
        if t0 is None:
            t0 = ts
        times.append(ts - t0)
        values.append(float(val))

plt.plot(times, values)
plt.xlabel("Time since first sample (s)")
plt.ylabel("Value")
plt.grid(True)
plt.tight_layout()
plt.show()
