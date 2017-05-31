#from neventarray import *

import errno
import numpy as np
import time
import binascii
import json

import zmq


class generatorSource:

    def __init__ (self,params,serialiser):
        self.port = params['port']
        self.context = zmq.Context()
        self.socket = self.connect()
        self.serialiser = serialiser()
        self.count = 0

    def connect(self):
        zmq_socket = self.context.socket(zmq.PUSH)
        zmq_socket.bind("tcp://*:"+self.port)
        zmq_socket.setsockopt(zmq.SNDHWM, 100)
        return zmq_socket

#    def mutation(self,ctl,d):
#        o = d
#        if (ctl.get('mutation') == 'nev') or (ctl.get('mutation') == 'all'):
#            if  np.random.rand() > .99 :
#                o = np.delete(o,np.random.rand(o.size))
#                print "Error: missing value"
#
#        if (ctl.get('mutation') == 'ts') or (ctl.get('mutation') == 'all'):
#            if  np.random.rand() > .99 :
#                o[1]['ts'] = -1
#                print "Error: wrong timestamp"
#
#        if (ctl.get('mutation') == 'pos') or (ctl.get('mutation') == 'all'):
#            if  np.random.rand() > .99 :
#                x=np.random.randint(o.size,size=np.random.randint(5)) 
#                o[1]["data"] = o[1]["data"] & 0xff000fff | 16773120
#                print "Error: wrong position"
#            if  np.random.rand() > .99 :
#                x=np.random.randint(o.size,size=np.random.randint(5)) 
#                o[2]["data"] = o[2]["data"] & 0xfffff000 | 4095
#                print "Error: wrong position"
#        return o

    
    def run(self,data,ctl,header):
        ctime=time.time()
        pulseID=0

        s = 1e-6*(data.nbytes)
        rate = float(ctl.get("rate"))
        print "size = ",s, "MB; expected bw = ",s * rate, "MB/s"

        while(ctl.get('run') != "stop"):
            print "here"
            stream_frequency = 1./rate

            itime = time.time()
            header["st"] = itime
            header["ts"] = 12345678
            header["pid"] = pulseID
            if ctl.get("run") != "pause":
                header["nev"] = data.shape[0]
                to_send = self.serialiser.create_from_dict(header,data)
            else:
                header["nev"] = 0
                to_send = self.serialiser.create_from_dict(header,[])

            def send_data(socket,data):
                print "here send"
                if ctl.get('run') == "run": 
                    socket.send(data[0],zmq.SNDMORE)
                    socket.send(data[1])
                else:
                    socket.send(data[0])
                self.count += 1
                print "here"

            send_data(self.socket,to_send)

            elapsed = time.time() - itime
            remaining = stream_frequency-elapsed
            print remaining

            if remaining > 0:
                time.sleep (remaining)
                print "here"

            pulseID += 1
            if time.time()-ctime > 10 :
                size = (data.nbytes+len(header))

                print "Sent ",self.count," events @ ",size*self.count/(10.*1e6)," MB/s"
                self.count = 0
                ctime = time.time()

                
