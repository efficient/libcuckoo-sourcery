#!/usr/bin/python
import sys
import numpy as np
#import matplotlib.pyplot as plt

if len(sys.argv) != 2:
    print './path_stats.py <output_file>'
    sys.exit()

file = open(sys.argv[1],'r')

dup = []
b_num = []
diff = 0
same = 0
while 1:
    line = file.readline()
    if not line:
        break
    if line.startswith('duplicated'):
        words  = line.split(',')
        _dup = int(words[0].split('=')[1])
        _num = int(words[1].split('=')[1])
        dup.append(_dup)
        b_num.append(_num)
        

print "number of paths:            ", len(dup)
print "number of duplications:     ", np.sum(dup), max(dup)
print "number of buckets:          ", np.sum(b_num), max(b_num)
print "duplication rate:           ", 1.0*np.sum(dup)/np.sum(b_num)

