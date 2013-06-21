#!/usr/bin/python
import sys
import numpy as np
import matplotlib.pyplot as plt

if len(sys.argv) != 2:
    print './path_stats.py <output_file>'
    sys.exit()

file = open(sys.argv[1],'r')

path_len = []
while 1:
    line = file.readline()
    if not line:
        break
    if line.startswith('cuckoo path length'):
        length = int(line.split(': ')[1])
        path_len.append(length)

keynum = len(path_len)

pLen = []
for i in range(0,10):
    pLen.append([])

seg = int(keynum*0.1)
for i in range(0,keynum):
    seg_id = min(int(i/seg),9)
    pLen[seg_id].append(path_len[i])

pLen_seg = []
pLen_min = []
pLen_max = []
pLen_mean = []

print 'all:  num = %d, min = %d, max = %d, mean = %.3f'%\
    (len(path_len), min(path_len), max(path_len), np.mean(path_len))

for i in range(0,10):
    pLen_seg.append(i+1)
    pLen_min.append(min(pLen[i]))
    pLen_max.append(max(pLen[i]))
    pLen_mean.append(np.mean(pLen[i]))
    print 'seg %d: num = %d, min = %d, max = %d, mean = %.3f'%\
    (i, len(pLen[i]), pLen_min[i], pLen_max[i], pLen_mean[i])
'''
outfile = open('%s_stat.py'%sys.argv[1],'w')
outfile.write('pLen_seg = '+str(pLen_seg) +'\n\n' \
              +'pLen_min = '+str(pLen_min) +'\n\n' \
              +'pLen_max = '+str(pLen_max) +'\n\n' \
              +'pLen_mean = '+str(pLen_mean))
'''
#print pLen_seg
#print pLen_min
#print pLen_max
#print pLen_mean

#plt.plot(pLen_seg, pLen_mean)
#plt.show()
