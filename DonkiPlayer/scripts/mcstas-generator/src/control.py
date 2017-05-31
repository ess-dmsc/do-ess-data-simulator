import os
import json
import threading
import time

# Set of classes that allow to control the status of the generator as well set
# the hardware status during run


class FileControl() :
    __ctl = {}
    size = 0
    __filename = ''
    
    def __init__(self,filename) :
        self.__filename = filename
        self.__run = True
        self.__handle = threading.Thread(target=self.__update,args=())
        self.__handle.daemon = True
        self.__handle.start()
        time.sleep(.1)

    def __del__(self) :
        self.__run = False
        self.__handle.join()
        
    def get(self, key):
        return self.__ctl[key]

    def int(self, key):
        return int(self.__ctl[key])

    def dump(self) :
        return self.__ctl
        
    def stop(self) :
        self.__run = False
    
    def __update(self) :
        while(self.__run) :
            with open(self.__filename) as f:
                for line in f.readlines() :
                    l = " ".join(line.split()).split()
                    self. __ctl[l[0]]=l[1]
            f.close()
            time.sleep(1)

    
class CommandLineControl() :
    __ctl = {}
    size = 0
    
    def __init__(self,filename) :
        with open(filename) as f:
            for line in f.readlines() :
                l = " ".join(line.split()).split()
                self. __ctl[l[0]]=l[1]
            f.close()
        self.__run = True
        self.__handle = threading.Thread(target=self.__update,args=())
        self.__handle.daemon = True
        self.__handle.start()

    def __del__(self) :
        self.__run = False
        self.__handle.join()
        
    def get(self, key):
        return self.__ctl[key]

    def int(self, key):
        return int(self.__ctl[key])

    def dump(self) :
        return self.__ctl
        
    def stop(self) :
        self.__run = False
    
    def __update(self) :
        while self.__run==True :
            value = raw_input('Enter your input:').split()
            print value
            time.sleep(5)

    
#class KafkaControl(object) :
#    __ctl = {}
#    def __init__(self,broker,topic) :
#        return false
#
