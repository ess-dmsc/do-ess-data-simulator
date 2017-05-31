from types import *

class circularBuffer():

#-----------------------------------------------------------------------------------
#    __init__
#-----------------------------------------------------------------------------------
    def __init__(self, Size):
	    self.size = Size
	    self.buffer = [None] * self.size
	    self.acq_counters = [None] * self.size
	    self.last_acq_counter = -1

#-----------------------------------------------------------------------------------
#    push_data
#-----------------------------------------------------------------------------------
    def push(self, acq_counter, datum):
	    index = acq_counter %  self.size
	    self.buffer[index] = datum
	    self.acq_counters[index] = acq_counter
	    self.last_acq_counter = acq_counter

#-----------------------------------------------------------------------------------
#    export_data
#-----------------------------------------------------------------------------------
    def export_data(self, first_index, last_index):
	resp = []
	if (last_index >= first_index):
		for idx in range(first_index,last_index+1):
			if type(self.buffer[idx]) is ListType:
				resp += self.buffer[idx]
			else:
				resp.append(self.buffer[idx])
	else:
		for idx in range(first_index,self.size):
			if type(self.buffer[idx]) is ListType:
				resp += self.buffer[idx]
			else:
				resp.append(self.buffer[idx])
		for idx in range(last_index+1):
			if type(self.buffer[idx]) is ListType:
				resp += self.buffer[idx]
			else:
				resp.append(self.buffer[idx])
        return resp

#-----------------------------------------------------------------------------------
#    get_last_data
#-----------------------------------------------------------------------------------
    def get_last_data(self, num):
	first_index = (self.last_acq_counter - num) % self.size + 1
	last_index = self.last_acq_counter % self.size
	return self.export_data(first_index, last_index)

#-----------------------------------------------------------------------------------
#    get_data_chunk
#-----------------------------------------------------------------------------------
    def get_data_chunk(self, first_counter, last_counter):
	first_index = first_counter % self.size
	last_index = last_counter % self.size
	if (self.acq_counters[first_index] == first_counter) and (self.acq_counters[last_index] == last_counter):
		return self.export_data(first_index, last_index)
	else:
		return[]

