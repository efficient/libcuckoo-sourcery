#!/usr/bin/python
import sys
import os
import numpy as np
import matplotlib.pyplot as plt

tput04 = [0]*86
tput08 = [0]*86
tput16 = [0]*86

tput_04 = []
tput_08 = []
tput_16 = []

os.chdir("/home/xiaozhou/Research/libcuckoo-dev/out_tput")
for filename in os.listdir("."):
    if filename.startswith("tput_04"):
        tput_04.append(filename)
    elif filename.startswith("tput_08"):
        tput_08.append(filename)
    elif filename.startswith("tput_16"):
        tput_16.append(filename)

for filename in tput_04:
    tput = open(filename, 'r')
    count = 0
    while 1:
        line = tput.readline()
        if not line:
            break
        if line.startswith('[bench] operations_tput'):
            t = float(line.split(' = ')[1].split()[0])
            tput04[count] += t
            count += 1

for filename in tput_08:
    tput = open(filename, 'r')
    count = 0
    while 1:
        line = tput.readline()
        if not line:
            break
        if line.startswith('[bench] operations_tput'):
            t = float(line.split(' = ')[1].split()[0])
            tput08[count] += t
            count += 1

for filename in tput_16:
    tput = open(filename, 'r')
    count = 0
    while 1:
        line = tput.readline()
        if not line:
            break
        if line.startswith('[bench] operations_tput'):
            t = float(line.split(' = ')[1].split()[0])
            tput16[count] += t
            count += 1


for i in range(0,86):
    tput04[i] = tput04[i]/10
    tput08[i] = tput08[i]/10
    tput16[i] = tput16[i]/10

load = np.arange(0.11, 0.97, 0.01)

ax = plt.subplot(111)

p1 = plt.plot(load, tput04, 'g-.')
p2 = plt.plot(load, tput08, 'b--')
p3 = plt.plot(load, tput16, 'k-')

plt.xlabel('Load factor')
plt.ylabel('Insert Throughput (MOPS)')
plt.yticks(np.arange(5, 45, 5))
#plt.xticks(np.arange(0.2, 0.96, 0.05))
plt.xlim(0.2,0.96)
plt.legend((p1[0], p2[0], p3[0]), ('4-way', '8-way', '16-way'), bbox_to_anchor=(0,0), loc=3, prop={'size':12})

ax.yaxis.grid(True, linestyle='--', which='major', color='lightgrey', alpha=0.5)
ax.xaxis.grid(True, linestyle='--', which='major', color='lightgrey', alpha=0.5)

plt.show()
