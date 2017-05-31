import flatbuffers
import random
import sys
from time import time
import numpy as np

sys.path.append('../src')
import Event as evt



class FlatbufferSerialiser(object) :
    builder = flatbuffers.Builder(0)

    def _set_up(self,htype,hws,ds,data) :
        self._string = self.builder.CreateString(htype)
        self._hws = evt.EventStartHwsVector(self.builder,len(hws))
        for i in hws[::-1] :
            self.builder.PrependUint16(i)
        self._hws = self.builder.EndVector(len(hws))
        self._ds = evt.EventStartHwsVector(self.builder,len(ds))
        for i in ds[::-1] :
            self.builder.PrependUint16(i)
        self._ds = self.builder.EndVector(len(ds))
        self._data = evt.EventStartDataVector(self.builder,len(data))
        for i in data:
            self.builder.PrependUint64(i)
        self._data = self.builder.EndVector(len(data))
        
    def _flatbuffer_add(self,ts,st,pid) :
        evt.EventStart(self.builder)
        evt.EventAddHtype(self.builder,self._string)
        evt.EventAddTs(self.builder,ts)
        evt.EventAddHws(self.builder,self._hws)
        evt.EventAddDs(self.builder,self._ds)
        evt.EventAddSt(self.builder,st)
        evt.EventAddPid(self.builder,pid)
        evt.EventAddData(self.builder, self._data)


    def create(self,htype,timestamp,hws,ds,sys_time,pid,data) :
        self._set_up(htype,hws,ds,data)
        self._flatbuffer_add(timestamp,sys_time,pid)
        ev = evt.EventEnd(self.builder)
        self.builder.Finish(ev)
        final_flatbuffer = self.builder.Output()
        return [final_flatbuffer,]

    def create_from_dict(self,head,data) :
        ds = []
        hws = np.zeros(5)
        for key, value in head["ds"][0].iteritems():
            ds.append(value)
        self._set_up(head["htype"],hws,ds,data)
        self._flatbuffer_add(head["ts"],head["st"],head["pid"])
        ev = evt.EventEnd(self.builder)
        self.builder.Finish(ev)
        final_flatbuffer = self.builder.Output()
        return [final_flatbuffer,]


class NoSerialiser(object) :

    def create(self,htype,timestamp,hws,ds,sys_time,pid,data) :
        rita2_header = {"htype":htype,"pid":pid,"st":sys_time,"ts":timestamp,"tr":100000,"ds":[{"ts":ds[1],"bsy":ds[2],"cnt":ds[3],"rok":ds[4],"gat":ds[5],"evt":ds[6],"id1":ds[7],"id0":ds[8]},len(data)],"hws":{"error":0,"overflow":0,"zmqerr":0,"lost":[0,1,2,3,4,5,6,7,8,9]}}
        return [rita2_header,data]

    def create_from_dict(self,head,data) :
        return [rita2_header,data]
