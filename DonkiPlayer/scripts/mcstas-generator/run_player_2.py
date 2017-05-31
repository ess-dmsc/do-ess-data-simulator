"""
Run McStas Generator

"""


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

def get_data_names():
    return []

def setup():
	msg = "Setup done"
	return msg
	
def stop():
	msg = "Stop done"
	return msg
  
def action(val=None):
	#os.system("python mcstasGeneratorMain.py -a sample/boa.2d -b optimusprime -s test-topic-0 -t sample/boa.tof")
	#print "player 2"

	params = {'topic': 'test-topic-2', 'broker': 'localhost'}
	surf = D2("/home/carlos/Documents/Elettra/Kafka_DonkiOrchestra_project/DonkiOrchestra_0.0.0/DonkiPlayer/scripts/mcstas-generator/sample/boa.2d")
	t = ToF("/home/carlos/Documents/Elettra/Kafka_DonkiOrchestra_project/DonkiOrchestra_0.0.0/DonkiPlayer/scripts/mcstas-generator/sample/boa.tof")
	multiplier = 1

	g = generatorSource(params,FlatbufferSerialiser)
	detector = Rita2(surf)
	
	ctl = FileControl('/home/carlos/Documents/Elettra/Kafka_DonkiOrchestra_project/DonkiOrchestra_0.0.0/DonkiPlayer/scripts/mcstas-generator/control.in')
	flags = np.array([ctl.int('evt'),ctl.int('bsy'),ctl.int('cnt'),ctl.int('rok'),ctl.int('gat')])
	
	stream = detector.mcstas2stream(flags)
	stream = np.tile(stream,multiplier)
	g.run(stream,ctl,rita2_header)
		   
	result = {}
	return result
##action()
