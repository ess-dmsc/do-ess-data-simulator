import unittest
import flatbuffers
import random
import sys
import os
from time import time
import numpy as np

sys.path.append('../src')
import Event as evt
import serialiser

final_flatbuffer = []

ds_value = np.array([32,1,1,1,1,4,12,12],dtype=np.dtype('u2'))
hws_value = np.array([0,0,0,0,0],dtype=np.dtype('byte'))

class TestFlatbufferCreation(unittest.TestCase):
    builder = flatbuffers.Builder(0)
    string = builder.CreateString("sinq-1.0")
    hws = evt.EventStartHwsVector(builder,5)
    for i in hws_value[::-1] :
        builder.PrependUint16(i)
    hws = builder.EndVector(5)
    
    ds = evt.EventStartHwsVector(builder,8)
    for i in ds_value[::-1] :
        builder.PrependUint16(i)
    ds = builder.EndVector(8)
    
    data = evt.EventStartDataVector(builder,1000)
    for i in np.array(np.linspace(1, 2**32,1000),dtype=np.dtype('u8')):
        builder.PrependUint64(i)
    data = builder.EndVector(1000)
    
    evt.EventStart(builder)
    evt.EventAddHtype(builder,string)
    evt.EventAddTs(builder,random.randint(1,1000))
    evt.EventAddHws(builder,hws)
    evt.EventAddDs(builder,ds)
    evt.EventAddSt(builder,time())
    evt.EventAddPid(builder,1)
    evt.EventAddData(builder, data)
    
    ev = evt.EventEnd(builder)
    builder.Finish(ev)
    final_flatbuffer = builder.Output()
    f = open('event.bin', 'wb')
    f.write(final_flatbuffer)
    f.close()
    
class TestFlatbufferReading(unittest.TestCase):

    f = open('event.bin', 'rb')
    raw_flatbuffer = f.read()
    f.close()
    
    os.remove('event.bin')
    event = evt.Event.GetRootAsEvent(bytearray(raw_flatbuffer),0)

    def test_ds(self): 
        for index in range(self.event.DsLength()):
            self.assertEqual(self.event.Ds(index),ds_value[index])

    def test_hws(self): 
        for index in range(self.event.HwsLength()):
            self.assertEqual(self.event.Hws(index),hws_value[index])

    def test_string(self) :
        self.assertEqual(self.event.Htype(),"sinq-1.0")

    def test_array(self) :
        self.assertEqual(self.event.DataLength(),1000)
        for i in range(self.event.DataLength()) :
            self.assertGreater(self.event.Data(i),0)
            
class TestFlatbufferSerialiser(unittest.TestCase) :
    fbs = serialiser.FlatbufferSerialiser()
    final_flatbuffer = fbs.create("sinq-1.0",random.randint(1,1000),hws_value,ds_value,time(),1,np.array(np.linspace(1, 2**32,1000),dtype=np.dtype('u8')))
    f = open('new_event.bin', 'wb')
    f.write(final_flatbuffer[0])
    f.close()

class TestFlatbufferSerialiserReading(unittest.TestCase):

    f = open('new_event.bin', 'rb')
    raw_flatbuffer = f.read()
    f.close()
    os.remove('new_event.bin')

    event = evt.Event.GetRootAsEvent(bytearray(raw_flatbuffer),0)

    def test_ds(self): 
        for index in range(self.event.DsLength()):
            self.assertEqual(self.event.Ds(index),ds_value[index])

    def test_hws(self): 
        for index in range(self.event.HwsLength()):
            self.assertEqual(self.event.Hws(index),hws_value[index])

    def test_string(self) :
        self.assertEqual(self.event.Htype(),"sinq-1.0")

    def test_array(self) :
        self.assertEqual(self.event.DataLength(),1000)
        for i in range(self.event.DataLength()) :
            self.assertGreater(self.event.Data(i),0)

            
if __name__ == '__main__':
    unittest.main()
