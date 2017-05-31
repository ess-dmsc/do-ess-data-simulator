#from neventarray import *

import errno
import numpy as np
import time
import binascii
import json
import sys

from struct import *

from confluent_kafka import Producer
from pykafka import KafkaClient,utils


class generatorSource:

    def __init__ (self,params,serialiser):
        self.params = params
        self.connect()
        self.serialiser = serialiser()
        self.count = 0
                
    def connect(self):
        conf = dict()
        client = KafkaClient(hosts=self.params['broker']+':9092')
        self.topic = client.topics[self.params['topic'] ]
##        conf_p = {  'bootstrap.servers': self.params['broker'],
##                    'message.max.bytes':20000000,
##                    'message.copy.max.bytes':20000000,
##                    'default.topic.config': {'message.timeout.ms': 1000, } }
##        self.pro = Producer(**conf_p)
        try :
            self.p = self.topic.get_producer(delivery_reports=True, sync=True)
        except e:
            print e.what()
        return   

    def mutation(self,ctl,d):
        o = d
        if (ctl.get('mutation') == 'nev') or (ctl.get('mutation') == 'all'):
            if  np.random.rand() > .99 :
                o = np.delete(o,np.random.rand(o.size))
                print "Error: missing value"

        if (ctl.get('mutation') == 'ts') or (ctl.get('mutation') == 'all'):
            if  np.random.rand() > .99 :
                o[1]['ts'] = -1
                print "Error: wrong timestamp"

        if (ctl.get('mutation') == 'pos') or (ctl.get('mutation') == 'all'):
            if  np.random.rand() > .99 :
                x=np.random.randint(o.size,size=np.random.randint(5)) 
                o[1]["data"] = o[1]["data"] & 0xff000fff | 16773120
                print "Error: wrong position"
            if  np.random.rand() > .99 :
                x=np.random.randint(o.size,size=np.random.randint(5)) 
                o[2]["data"] = o[2]["data"] & 0xfffff000 | 4095
                print "Error: wrong position"

        return o



    def run(self,data,ctl,header):
        print "len of data before send it", len(data), "bytes",data.nbytes
        ctime=time.time()
        pulseID=0

        s = 1e-6*(data.nbytes)
        print "size = ",s, "MB; expected bw = ",s * ctl.int("rate"), "MB/s"

        while(self.count <= 10):

            stream_frequency = 1./ctl.int("rate")
            header["st"] = int(time.time())
            header["ts"] = 12345678
            header["pid"] = pulseID
            
            if ctl.get("run") != "pause":
                header["nev"] = data.shape[0]
                to_send = self.serialiser.create_from_dict(header,data)
            else:
                header["new"] = 0
                to_send = self.serialiser.create_from_dict(header,[])

            def delivery_callback (err, msg):        
                if err:
                    print "Message failed delivery: \n", err
                else:
                    print "Message delivered to", msg.topic(), "\n" , msg.partition()
                
            def send_data(producer,data):
##                
                for i in data :
##                    print type(i), len(i)
                    producer.produce(str(np.frombuffer(i)))                    
##                    print np.frombuffer(i)
                    print "sent", self.count
##                    self.pro.produce(topic=self.params['topic'], value=str(np.frombuffer(i)), callback=delivery_callback)
                    
                self.count += 1

            send_data(self.p,to_send)
##            send_data(to_send)

            elapsed = time.time() - header["st"]
            remaining = stream_frequency-elapsed
            
            if remaining > 0:
                time.sleep (remaining)

            pulseID += 1
            if time.time()-ctime > 10 :
                size = (data.nbytes+len(header))
                
                print "Sent ",self.count," events @ ",size*self.count/(10.*1e6)," MB/s"
                self.count = 0
                ctime = time.time()

                
