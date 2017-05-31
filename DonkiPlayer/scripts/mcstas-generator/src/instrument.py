import numpy as np
import csv
import re
import json
import numpy.random as random
import ritaHeader

# RITA2 instrument consists of an area detector. The event timestamp is randomly
# generated. Data is in agreement with sinq-1.0 format

event_t = np.dtype(np.uint64)

class Rita2:
    header = ritaHeader.rita2_header
    print "header:", header
    
    def __init__ (self,area):
        self.area = area
        self.d = np.empty(area.count(),dtype=event_t)

    def mcstas2stream(self,flags):
        sz = self.area.n.shape
        count = 0
        bin_flags = flags[0] << 24 | flags[1] << 28 | flags[2] << 29 | flags[3] << 30 | flags[4] << 31
        for x in range(sz[0]):
            for y in range(sz[1]):
                value = np.uint32(x) | np.uint32(y) << 12
                for n in range(int(self.area.n[x][y])):
                    self.d[count] = value | bin_flags | random.randint(2**31) << 32
                    count += 1
        return self.d
