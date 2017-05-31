import sys
import os
import errno

import numpy as np

import nxs
import neventarray
import time

def loadAMOR(source) :
    f = nxs.open(source,'r')

    f.openpath("entry1/AMOR/area_detector/data")
    dim,datatype= f.getinfo()
    size = np.prod(dim)
    data = f.getdata()
    f.close()

    f = nxs.open(source,'r')
    f.openpath("entry1/AMOR/area_detector/time_binning")
    tof = f.getdata()
    f.close()

    nEvents = np.sum(data)
    signal = np.empty(nEvents,dtype=neventarray.event_t)

    nEv = 0
    detID = 0
    for row in range(dim[0]):
        for col in range(dim[1]):
            for it in range(dim[2]):
                events = data[row][col][it]
                if events > 0:
                    signal[nEv:nEv+events]["ts"] = round(tof[it]/10.)
                    signal[nEv:nEv+events]["x"] = row
                    signal[nEv:nEv+events]["y"] = col
                    nEv = nEv+events

    if not signal.shape[0] == nEv:
        raise Exception("Error in reading NeXus data: wrong number of events",nEv)

    return signal






############################
def loadFOCUS(source) :
    f = nxs.open(source,'r')

    f.openpath("entry1/FOCUS/merged/counts")
    dim,datatype= f.getinfo()
    size = np.prod(dim)
    data = f.getdata()
    f.close()

    f = nxs.open(source,'r')
    f.openpath("entry1/FOCUS/merged/time_binning")
    tof = f.getdata()
    f.close()

    nEvents = np.sum(data)
    signal = np.empty(nEvents,dtype=neventarray.event_t)

    nEv = 0
    detID = 0
    for row in range(dim[0]):
        for it in range(dim[1]):
            events = data[row][it]
            if events > 0:
                signal[nEv:nEv+events]["x"] = row
                signal[nEv:nEv+events]["y"] = it
                nEv = nEv+events

    if not signal.shape[0] == nEv:
        raise Exception("Error in reading NeXus data: wrong number of events",nEv)

    return signal
##########################


def loadRITA2(source) :

    f = nxs.open(source,'r')

    f.openpath("entry1/RITA-2/detector/counts")
    dim,datatype= f.getinfo()
    size = np.prod(dim)
    data = f.getdata()
    f.close()

    np.random.seed(1234)
    timestamp = np.sort(np.random.randint(2**32-1,size=dim[0]))

    nEvents = np.sum(data)

    signal = np.empty(nEvents,dtype=neventarray.event_t)

    nEv = 0
    detID = 0

    row = np.uint16
    col = np.uint16

    m = np.sum(data,0)
    np.savetxt('orig.out',m,delimiter=' ')

    for row in range(dim[1]):
        for col in range(dim[2]):
            for it in range(dim[0]):
                events = data[it][row][col]
                if events > 0:
                    signal[nEv:nEv+events]["ts"] = timestamp[it]
                    signal[nEv:nEv+events]["data"] = (0 << 31 | 0 << 30 | 1 << 29 | 1 << 28 | 2 << 24 | col << 12 | row ) 
                    nEv = nEv+events


    return signal


def header(pulseID=1234,st=time.time(),ts=np.random.randint(3200000000),ne=0):
    dataHeader = '{"htype\":"sinq-1.0","pid":'+str(pulseID)+',st":'+str(st)+',"ts":'+str(ts)+',"tr":100000,"ds":[{"ts":32,"bsy":1,"cnt":1,"rok":1,"gat":1,"evt":4,"id1":12,"id0":12},'+str(ne)+'],"hws":{"error":0,"overflow":0,"zmqerr":0,"lost":[0,1,2,3,4,5,6,7,8,9]}}\0'

    return dataHeader


def loadNeXus2event(source):

    if not os.path.isfile(source) :
        raise IOError

    print "Loading from file " + source
    if "amor" in source:
        return loadAMOR(source)
    if "rita" in source:
        return loadRITA2(source)

    raise NotImplementedError("Detector not implemented")



def event2debug(d):
    o = np.empty([2*d.size,1],dtype=neventarray.event_t)
    dev = 0

#    for count in range(d.size):        
#        o[2*count  ]["ts"] = d[count]["ts"]
#        o[2*count+1]["ts"] = d[count]["ts"]
#        o[2*count  ]["other"] = ( (d[count]["sync"]       << 16) + 
#                                  (np.random.randint(2)*2 << 12) +
#                                  (d[count]["y"]          <<  0) );
#        o[2*count+1]["other"] = ( (d[count]["sync"]       << 16) + 
#                                  (1                      << 12) +
#                                  (d[count]["x"]          <<  0) );
#
#    for c in o[:10]:
#        print c

    return o
