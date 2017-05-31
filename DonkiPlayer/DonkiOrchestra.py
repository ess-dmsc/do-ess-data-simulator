import zmq
import numpy

#-----------------------------------------------------------------------------------
#    push_data :
#		
#-----------------------------------------------------------------------------------
def push_data(socket, tag, trg_start, trg_stop, data_value):
	# At the moment just use send_pyobj	
	socket.send_pyobj([tag.lower(), trg_start,trg_stop,data_value])

#-----------------------------------------------------------------------------------
#    push_trigger :
#		
#-----------------------------------------------------------------------------------
def push_trigger(socket, trigger_value, priority):
	# At the moment just use send_pyobj	
	socket.send_pyobj(["trigger", trigger_value, priority])

