#from neventarray import *

import sys, os, getopt
import errno
import numpy as np
import ast

sys.path.append('./src')

from detector import *

from instrument import Rita2

from ritaHeader import control
from ritaHeader import rita2_header
from control import FileControl
from serialiser import FlatbufferSerialiser

import zmqGenerator
import kafkaGenerator

##from zmqGenerator import generatorSource
from kafkaGenerator import generatorSource

multiplier = 1

params=dict()

def usage() :
    print '\nUsage:'
    print '\tpython mcstasGeneratorMain.py -a <area detector file(s)> -t <tof detector file(s)> broker port [multiplier]\n'
    print '\tpython mcstasGeneratorMain.py -h'
    print ''
    print '-a,--area :\tfile (or list of,comma separated) containing area detector mcstas output'
    print '-t,--tof :\tfile (or list of,comma separated) containing tof mcstas output'
    print 'broker, port :\tkafka broker and port or TCP port on which 0MQ will stream data (broker ignored in this case)'
    print 'multiplier :\tincrease data size using <multiplier> identical copies in the same blob (optional, default = 1)'
    print '-h,--help :\tthis help'



def mainRITA2(surf):
##    print generatorSource
##    print zmqGenerator.generatorSource    
    print "surf[0]", surf
    if generatorSource == zmqGenerator.generatorSource :
        try :
            params['port']
        except ValueError:
            print "Error: missing port number"
    if generatorSource == kafkaGenerator.generatorSource :
        try :
            params['broker']
            params['topic']
        except ValueError:
            print "Error: missing broker or topic"
    print "params", params
    g = generatorSource(params,FlatbufferSerialiser)
    detector = Rita2(surf)

    ctl = FileControl('control.in')
    flags = np.array([ctl.int('evt'),ctl.int('bsy'),ctl.int('cnt'),ctl.int('rok'),ctl.int('gat')])

    stream = detector.mcstas2stream(flags)
    print "stream data", type(stream), stream.shape
##    print stream[0:100]
    
    stream = np.tile(stream,multiplier)
    print "rita2_header", rita2_header
    g.run(stream,ctl,rita2_header)



# rather general main function
if __name__ == "__main__":
    try:
        opts,args = getopt.getopt(sys.argv[1:], "t:a:b:s:p:m:h",["tof","area","broker","source","port","multiplier","help"])
    except getopt.GetoptError as err:
        print str(err)
        usage()
        sys.exit(2)
        
    for o,arg in opts:
##        print o, arg.split(',')
        if o in ("-h","--help"):
            usage()
            sys.exit()
        if o in ("-t","--tof"):
            all_arguments = arg.split(',')
            print "all", all_arguments
            t = [ToF(s) for s in all_arguments]
            for s in all_arguments:
                print "s, tof", s, type(s)
            print "t", t
        if o in ("-a","--area"):
            all_arguments = arg.split(',')
            surf = [D2(s) for s in all_arguments]
            for s in all_arguments:
                print "s, area", s
        if o in ("-b","--broker"):
            params['broker'] = arg
        if o in ("-s","--source"):
            params['topic'] = arg
        if o in ("-p","--port"):
            params['port'] = arg
        if o in ("-m","--multiplier"):
            multiplier = int(arg)

##        print "surf:", surf, "type:", type(surf), "len:", len(surf)
##        print type(surf[0])
    print "surf", surf[0]
    mainRITA2(surf[0])



