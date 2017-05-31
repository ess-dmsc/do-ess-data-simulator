# -*- coding: utf-8 -*-
"""
The main class here is MetaDataHandler that will allow collecting the 
metadata information for the hdf5 files from a single object.

"""
from xmlconfig import DaqXmlConfig

##import PyTango
import sys
import threading
import time
import traceback


class InvalidEntry(Exception): 
    """
    Inform that there is an invalid entry for the metadata. 
    
    It does not accept for example invalid tango attribute.
    """    
    pass


class StaticMetadata():
    """
    Auxiliar class to allow the same fancy way to collect metadata.
    
    It exists to give to externals, the same behaviour of DynamicMetaData
    when you want to get the value of the MetaData object.
    """    
    def __init__(self, value):
        self.value = value
        self.is_paused = False

    def state(self):
        if self.value != None:
            return PyTango.DevState.ON
        else:
            return PyTango.DevState.FAULT


class DynamicMetadata():
    """
    Allows the possibility to read the metadata in a fancy way. 
    
    It is possible to read the tango attribute through ::
       
       a = dynamic_metadata.value # returns the current tango attribute.
    
    """
    def __init__(self, entry):
        self.entry_name = entry
        self.proxyOK = False
        self.do_read = False
        self.go_on = True
        self.is_paused = False
        self.last_value = ""
        self.update_value()

        
    def state(self):
        if self.last_value != None:
            return PyTango.DevState.ON
        else:
            return PyTango.DevState.FAULT

    def _init_connection(self, entry):
        try:
            self.entry = PyTango.AttributeProxy(entry)
            self.entry.get_device_proxy().set_timeout_millis(1000)
            self.proxyOK = True
        except PyTango.DevFailed, ex: 
            #sys.stderr.write("Unable to read metadata entry %s\n"% entry)
            #PyTango.Except.print_exception(ex)
            self.proxyOK = False
            #raise InvalidEntry(str(ex))
            self.last_value = None
        
    def update_value(self):
        if self.is_paused:
            return
        if self.proxyOK == False:
            self._init_connection(self.entry_name)
        try:
            if self.proxyOK:
                self.last_value = self.entry.read().value
        except PyTango.Except, ex:
            #PyTango.Except.print_exception(ex)
            self.last_value = None
        except :
            self.last_value = None

    @property
    def value(self):
        """
        Property for allowing reading the tango attribute using object.value.
                        
        """
        return self.last_value

        

class MetaDataHandler(threading.Thread):
    """
    Do management of all MetaData for the HDF5 files. 
    
    Basically, there are two kind of metadata, those that 
    are give some information specific for a dataset, that are called 
    metadata, and those that give some information of the experiment, that 
    may, some times, be collected as datasets. 
    
    There are also two kind of metadata related to their nature, they may be
    static, in the sense that they do not change for the wholle acquisition 
    shift, or dynamic, in the sense, that it should be read some how, 
    periodically. 
    
    This class allows to have a central object that will give back all 
    the metadata for the HDFwriters. 
    
    
    This class will provide two attributes to handle the metadata. ::meta, is 
    the one that usually will produce metadata for the datasets. It is a 
    dictionary, the dictionary entries have two fiels: the metadata name, 
    and a Dynamic or StaticMetadata object. So, imagine that there is a 
    dataset called: scalar/area::
    
       dset_metadata = metadatahandler.meta['scalar/area']
    
    this dset_metadata will be a list of tuple. For each entry in the list, 
    there will be the metadata name and the object whose value is the metadata 
    value. 
    
    To be more clear, look at this code, that uses the metadatahandler to 
    produce the attributes of the dataset scalar/area::
        
        dset = h5file.create_dataset('scalar/area',data=area_values)
        for meta_entry in metadatahandler.meta['scalar/area']:
            dset.attr[meta_entry[0]] = meta_entry[1].value
    
    The MetaDataHandler also provide the ::meta_attributes that cope 
    with the metadata information that will be datasets in the hdf5. 
    
    They are dictionaries that has just an object of Metadata 
    (Static or Dynamic). The following code shows how to write these datasets
    inside an hdf5 file::
 
        for (meta_dset_name,meta_dset_val) in metadatahandler.meta_attribute.items():
            hdf5file.create_dataset(meta_dset_name, data=meta_dset_val.value)
    
    """
    def __init__(self, daq_xml_config):
        threading.Thread.__init__(self)
        assert isinstance(daq_xml_config, DaqXmlConfig)

        self.meta = dict() # metadata for the datasets
        self.meta_attribute = dict()
        self.do_read = False
        for (daq_sync_key, daq_sync_value) in daq_xml_config.sync.items():
            l_meta_entry = []            
            # Try reading metadata in the Tango attribute conf
            try:
                tango_entry = PyTango.AttributeProxy(daq_sync_value.tango_attr)            
                tango_entry.get_device_proxy().set_timeout_millis(1000)
                conf = tango_entry.get_config()                        
                if conf.unit != 'No unit':
                    l_meta_entry += [('unit', StaticMetadata(conf.unit))]
                if conf.description != 'No description':
                    l_meta_entry += [('description', StaticMetadata(conf.description))]
            except PyTango.DevFailed, ex: 
                print "**** Unable to read conf from " , daq_sync_value.tango_attr
                #PyTango.Except.print_exception(ex)
                #raise InvalidEntry(str(ex))
    
            # Try setting up additional metadata readers
            for (meta_key, meta_entry) in daq_sync_value.metadata.items():
                if meta_entry.dynamic:
                    l_meta_entry += [(meta_key, DynamicMetadata(meta_entry.tango_attr))]
                else:
                    try:
                        vval = PyTango.AttributeProxy(meta_entry.tango_attr).read()                    
                        l_meta_entry += [(meta_key, StaticMetadata(vval.value))]
                    except PyTango.DevFailed, ex: 
                        print "**** Unable to read metadata value from " , meta_entry.tango_attr
                        #PyTango.Except.print_exception(ex)
                        #raise InvalidEntry(str(ex))
            if l_meta_entry:
                self.meta[daq_sync_key] = l_meta_entry
        
        for (daq_meta_key, daq_meta_value) in daq_xml_config.meta.items():
            tango_attr_name = daq_meta_value.tango_attr
            #FIXME
            if daq_meta_value.dynamic or True:
                self.meta_attribute[daq_meta_key] = DynamicMetadata(tango_attr_name)
            else:
                try:
                    val = PyTango.AttributeProxy(daq_meta_value.tango_attr).read()
                    self.meta_attribute[daq_meta_key] = StaticMetadata(val.value)
                except PyTango.DevFailed, ex: 
                    print "**** Unable to read metadata value from " , daq_meta_value.tango_attr
                    #PyTango.Except.print_exception(ex)
                    #raise InvalidEntry(str(ex))
            
    def run(self):
        self.last_read_time = time.time()
        self.go_on = True
        while self.go_on:
            try:
                # pollong loop
                poll_period_sec = 1
                if self.do_read and (time.time() - self.last_read_time) > poll_period_sec:
                    self.last_read_time = time.time()
                    for key,meta_attr in self.meta_attribute.iteritems():
                        if isinstance(meta_attr, DynamicMetadata):
                            meta_attr.update_value()
                    #start polling on shot2shot source metadata
                    for attr_meta_list in self.meta.iteritems():
                        for key,meta_attr in attr_meta_list[1]:
                            if isinstance(meta_attr, DynamicMetadata):
                                meta_attr.update_value()
                else:
                    time.sleep(0.01)
            except:
                print traceback.format_exc()

    
    def serve_task_queue(self):
        try:
            next_task = self.task_queue.get()
            if next_task[0] == 'start_polling':
                self.start_polling()
            elif next_task[0] == 'stop_polling':
                self.stop_polling()
            elif next_task[0] == 'read_metadata':
                daq_meta_key = next_task[1]
                self.data_queue.put({daq_meta_key:self.meta_attribute[daq_meta_key].value})
            elif next_task[0] == 'pause_daq':
                daq_meta_key = next_task[1]
                do_pause = next_task[2]
                self.meta_attribute[daq_meta_key].is_paused = do_pause
            elif next_task[0] == 'stop':
                self.go_on = False
            elif next_task[0] == 'daq_meta_needed':
                hdf_writer_key = next_task[1]
                daq_key = next_task[2]
                meta_attrs = {}
                if daq_key in self.meta:
                    attrib_metadata = self.meta[daq_key]
                    for (key,meta_entry) in attrib_metadata:
                        meta_value = meta_entry.value
                        if meta_value == None:
                            print "Error: " + daq_key + " metadata readout failed, check device "+ meta_entry.entry_name + "\n"
                            meta_value = ""
                        meta_attrs[key] = meta_value
                self.data_queue.put({hdf_writer_key:[daq_key,meta_attrs]})
            elif next_task[0] == 'meta_attrs_needed':
                hdf_writer_key = next_task[1]
                meta_attrs={}
                for key,meta_attr in self.meta_attribute.iteritems():
                    if not meta_attr.is_paused:
                        meta_attrs[key] = meta_attr.value
                self.data_queue.put({hdf_writer_key:meta_attrs})
        except:
            print traceback.format_exc()


    def start_polling(self):
        self.do_read = True

    def stop_polling(self):
        self.do_read = False

    def __str__(self):
        return "MetaDataHandler => meta = %s attribs = %s"% ((str(self.meta),str(self.meta_attribute)))
        
                
