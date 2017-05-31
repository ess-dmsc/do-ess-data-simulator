import sys
import os
import errno
import time
import json
import numpy as np

rita2_header = {"htype":"sinq-1.0","pid":0,"st":12345678,"ts":987654,"tr":100000,"ds":[{"ts":32,"bsy":1,"cnt":1,"rok":1,"gat":1,"evt":4,"id1":12,"id0":12},0],"hws":{"error":0,"overflow":0,"zmqerr":0,"lost":[0,1,2,3,4,5,6,7,8,9]}}


def header(pid=1234,st=time.time(),ts=np.random.randint(3200000000),ne=0):
    with open("header.in") as i:
        h = json.load(i)
    i.close()

    h["pid"]   = pid
    h["st"]    = st
    h["ts"]    = ts
    h["ds"][1] = ne
    
    return json.dumps(h)
    

def control(str):
    with open(str) as i:
        ctl = json.load(i)
    i.close()
    return ctl

def set_ds(d,ctl):
    for i in d:
        i["data"] = (i["data"] & 0xffffff) | (ctl["bsy"] << 31 | ctl["cnt"] << 30 | ctl["rok"] << 29 | ctl["gat"] << 28 | ctl["evt"] << 24)
    return d
