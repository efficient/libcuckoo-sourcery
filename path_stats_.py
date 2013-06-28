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

keynum = pow(2,20)*4

pLen = []
for i in range(0,5):
    pLen.append([])

for i in range(0,len(path_len)):
    if i < keynum*0.5:
        pLen[0].append(path_len[i])
    elif i< keynum*0.8:
        pLen[1].append(path_len[i])
    elif i< keynum*0.9:
        pLen[2].append(path_len[i])
    elif i< keynum*0.94:
        pLen[3].append(path_len[i])
    else:
        pLen[4].append(path_len[i])

print 'all:  num = %d, min = %d, max = %d, mean = %.3f %d %d %d %d %d'%\
    (len(path_len), min(path_len), max(path_len), np.mean(path_len),\
         np.percentile(path_len,50), np.percentile(path_len,75), np.percentile(path_len,90), np.percentile(path_len,95), np.percentile(path_len,99))

for i in range(0,5):
    print 'seg %d: num = %d, min = %d, max = %d, mean = %.3f %d %d %d %d %d'%\
    (i, len(pLen[i]), min(pLen[i]), max(pLen[i]), np.mean(pLen[i]), \
     np.percentile(pLen[i],50), np.percentile(pLen[i],75), np.percentile(pLen[i],90),\
     np.percentile(pLen[i],95), np.percentile(pLen[i],99))
