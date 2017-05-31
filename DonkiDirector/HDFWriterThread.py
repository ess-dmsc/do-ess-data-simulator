import time
import threading 
import PyTango
import numpy
import h5py

THREAD_DELAY_SEC = 0.1

class HDFwriterThread(threading.Thread):

#-----------------------------------------------------------------------------------
#    __init__
#-----------------------------------------------------------------------------------
    def __init__(self, parent_obj, filename_in, trg_start, trg_stop):
        threading.Thread.__init__(self)
	self._alive = True
	self.myState = PyTango.DevState.OFF
	self.filename = filename_in
	self.parent = parent_obj
	self.trg_start = trg_start
	self.trg_stop = trg_stop
	self.data_queue = []
	self.datasource_finished = {}
	self._hdf_file = None
	self.timeout_sec = 20
	self.MetadataSources = {}
	if "_errors" in dir(h5py):
		h5py._errors.silence_errors()
		


#-----------------------------------------------------------------------------------
#    set_Metadata_Sources
#-----------------------------------------------------------------------------------
    def set_Metadata_Sources(self, MetadataSources):
    	self.MetadataSources = MetadataSources

#-----------------------------------------------------------------------------------
#    notify_new_data
#-----------------------------------------------------------------------------------
    def notify_new_data(self, daq_thread, trg):
    	self.data_queue.append([daq_thread, trg])


#-----------------------------------------------------------------------------------
#    store_metadata
#-----------------------------------------------------------------------------------
    def store_metadata(self):
    	for metakey in self.MetadataSources.keys():
		if not self.MetadataSources[metakey]['enabled']:
			continue
		try:
			attprx = PyTango.AttributeProxy(self.MetadataSources[metakey]['tango_attr'])
			attrinfo = attprx.get_config()
			attprx.get_device_proxy().set_timeout_millis(500)
			data_in = attprx.read().value
			del attprx
		except Exception, ex:
			self.MetadataSources[metakey]['status'] = 'ALARM'
			print "store_metadata, attribute proxy",metakey,ex
			continue
			#
		retries = 0
		while retries < 3:
			if metakey in self._hdf_file:
				break
			try:
				# Create HDF dataset
		    		dset = self._hdf_file.create_dataset(metakey, data=data_in)
				#dset = self._hdf_file[metakey]
				dset.attrs["unit"] = attrinfo.unit
				break
			except Exception, ex:
				print "store_metadata",metakey,self.trg_start,ex
				retries += 1


#-----------------------------------------------------------------------------------
#    store_sync_player_metadata
#-----------------------------------------------------------------------------------
    def store_sync_player_metadata(self,daq_thread):
    	player_metadata = daq_thread.player_metadata
	dset = self._hdf_file[daq_thread.player_nickname]
    	for key in player_metadata.keys():
		try:
			attprx = PyTango.AttributeProxy(player_metadata[key].tango_attr)
			attprx.get_device_proxy().set_timeout_millis(500)
			data_in = attprx.read().value
			del attprx
			#
			dset.attrs[key] = data_in
		except Exception, ex:
			print "store_sync_player_metadata",key,ex
	#
	# Unit is default
	try:
		attprx = PyTango.AttributeProxy(daq_thread.player_attrname)
		attrinfo = attprx.get_config()
		del attprx
		#
		dset.attrs["unit"] = attrinfo.unit
	except Exception, ex:
		print "store_sync_player_metadata, deafult unit",daq_thread.player_attrname,ex

#-----------------------------------------------------------------------------------
#    store_data
#-----------------------------------------------------------------------------------
    def store_data(self, daq_queue_item):
    	daq_thread = daq_queue_item[0]
	trg = daq_queue_item[1]
	data_in = daq_thread._data_buffer[trg]
	if data_in == None:
		return
	if daq_thread.player_nickname not in self.datasource_finished.keys():
		self.datasource_finished[daq_thread.player_nickname] = False
		#
		# Create HDF dataset
		tokens = daq_thread.player_nickname.split("/")
		groupname=""
		dsetname = daq_thread.player_nickname
		dataset_len = 1+self.trg_stop-self.trg_start
		retries = 0
		while (retries < 3):
			try:
			    if dsetname in self._hdf_file:
			    	break
			    if len(numpy.shape(data_in)) == 0: #scalar
		      		self._hdf_file.create_dataset(dsetname,shape=(dataset_len,),dtype=numpy.dtype(type(data_in)))
			    elif len(numpy.shape(data_in)) == 1: #spectrum
				self._hdf_file.create_dataset(dsetname, shape=(dataset_len,data_in.shape[0]), dtype=data_in.dtype)
			    elif len(numpy.shape(data_in)) == 2: #image
		      		self._hdf_file.create_dataset(dsetname, shape=(dataset_len,data_in.shape[0],data_in.shape[1]), dtype=data_in.dtype)
			    break
        		except Exception, ex:
				print "Create Dataset",dsetname,data_in,len(numpy.shape(data_in)),dataset_len,"\n",ex
				retries += 1
	    	#
		self.store_sync_player_metadata(daq_thread)
        #
	retries = 0
	while (retries < 3):
            #update the dataset
            try:
	       	dset = self._hdf_file.get(daq_thread.player_nickname, None)
		dset[slice(trg - self.trg_start,trg - self.trg_start+1)] = data_in
		break
            except Exception, ex:
	    	retries += 1
		print "Update Dataset",ex
        #                  
	if trg == self.trg_stop:
		self.datasource_finished.pop(daq_thread.player_nickname)

#-----------------------------------------------------------------------------------
#    close_file
#-----------------------------------------------------------------------------------
    def close_file(self):
	try:
		#
		data_in=numpy.arange(self.trg_start,self.trg_stop+1)
		self._hdf_file.create_dataset("triggers", data = data_in)
		#
		self.store_metadata()
		#
		self._hdf_file.flush()
		self._hdf_file.close()
		self.parent.report_message("Closed file "+self.filename)
	except Exception, ex:
		print "Closing File",ex
	self.parent.notify_hdf_file_finished(self)

#-----------------------------------------------------------------------------------
#    run
#-----------------------------------------------------------------------------------
    def run(self):
	try:
		self._hdf_file = h5py.File(self.filename,'w')
		self.parent.report_message("Opened file "+self.filename)
	except Exception, ex:
		print ex
		self.parent.report_message("Unable to Open file "+self.filename)
		return
	last_store_time = time.time()
	while self._alive:
	    while len(self.data_queue):
	    	#
	    	self.store_data(self.data_queue[0])
		del self.data_queue[0]
		last_store_time = time.time()
		#
		if len(self.datasource_finished) == 0:
			self.close_file()
	    if self.parent._paused:
	    	last_store_time = time.time()
	    elif (time.time() - last_store_time) > self.timeout_sec:
	    	print "TIMEOUT",self.filename
		self.close_file()
		last_store_time = time.time()
	    time.sleep(THREAD_DELAY_SEC)

