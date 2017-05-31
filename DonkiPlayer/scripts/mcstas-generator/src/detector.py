import numpy as np
import csv
import re
import struct

# Basic classes that allow to read the output of a mcstas simulation (Format:
# McStas with text headers file) and extract the neutrons count
# Constructors receives the name of the mcstas output file

def mcstasHeader(str):
    reader = csv.reader(open(str,"r"), delimiter=' ')
    return [row[1:] for row in reader if re.match('#', row[0], re.IGNORECASE)]
    
def float32_bit_pattern(value):
    return sum(ord(b) << 8*i for i,b in enumerate(struct.pack('f', value)))

def int_to_binary(value, bits=32):
    return bin(value).replace('0b', '').rjust(bits, '0')


##########################
# ToF detector    
class ToF:
    def __init__ (self,str) :
        self.t,self.n = self.read(str)
        h = mcstasHeader(str)
        self.Ncount = int(float([x for x in h if x[0].strip(':') == 'Ncount'][0][1]))
        self.xmin  = float([x for x in h if x[0].strip(':') == 'xlimits'][0][1])
        self.xmax  = float([x for x in h if x[0].strip(':') == 'xlimits'][0][2])

    def read(self,str) :
        d,n = np.loadtxt(str,usecols=(0,3),unpack=True) # tof & n count
        d = map(float32_bit_pattern,d)
        return d,n

    def count(self):
        return sum(self.n)

    # Hack
    def randomize(self,val) :
        self.n = np.random.random(size=val)*(self.xmax-self.xmin)+self.xmin
        m = np.amin(self.n)
        if m < 0:
            self.n -= m-1

##########################
# 2-D detector    
class D2:
    def __init__ (self,str) :
        self.n = self.read(str)
        self.h = mcstasHeader(str)
        self.Ncount = int(float([x for x in self.h if x[0].strip(':') == 'Ncount'][0][1]))

    def read(self,str) :
        with open(str) as fp:
            for line in fp:
                if line.split(' ')[1] == 'type:' :
                    nx = int(line[(line.index('(')+1):line.index(',')])
                    ny = int(line[(line.index(',')+1):(line.index(')'))])
        
        d = np.loadtxt(str,unpack=True)
        return d[:nx,2*ny:3*ny]

    def count(self):
        return sum(sum(self.n))



##########################
# N-D detector (TODO)
#class ND:
#    def __init__(self):
