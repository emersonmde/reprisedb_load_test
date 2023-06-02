#!/usr/bin/env python3
import matplotlib.pyplot as plt
import pandas as pd

# Load the data from the CSV file
data = pd.read_csv('metrics.csv')

# Plot the metrics
fig, ax1 = plt.subplots()

color = 'tab:blue'
# Make the y-axis label, ticks and tick labels match the line color.
ax1.set_xlabel('time (s)')
ax1.set_ylabel('operations', color=color)
ax1.plot(data['time'], data['read_ops'], color='tab:blue', label='Read Ops')
# ax1.plot(data['time'], data['write_ops'], color='tab:orange', label='Write Ops')
ax1.tick_params(axis='y', labelcolor=color)

ax2 = ax1.twinx()  # instantiate a second axes that shares the same x-axis

color = 'tab:red'
# We already handled the x-label with ax1
ax2.set_ylabel('avg ops/s', color=color)
ax2.plot(data['time'], data['read_ops_per_sec'], color='tab:red', label='Avg Read/s')
ax2.plot(data['time'], data['write_ops_per_sec'], color='tab:green', label='Avg Write/s')
# ax2.plot(data['time'], data['memtable_size'], color='tab:purple', label='Memtable Size')
ax2.tick_params(axis='y', labelcolor=color)

# Make sure both y-axes start at 0
ax1.set_ylim(bottom=0)
ax2.set_ylim(bottom=0)

# Create a legend
fig.legend(loc="upper right", bbox_to_anchor=(1,1), bbox_transform=ax1.transAxes)

fig.tight_layout()  # otherwise the right y-label is slightly clipped
plt.show()
