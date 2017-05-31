# -*- coding: utf-8 -*-

import sys
import threading
import multiprocessing
import time
import traceback
import zmq
import numpy
from hdfwriter import HDFWriter
from metadatahandler import MetaDataHandler


HDFWTHREADS = 1
MAX_HDFWTHREADS = 3

HDF5FILESIZE = 15

_dbg = True 

class DataServer(multiprocessing.Process):
    """
    Do management of all Data for the HDF5 files. 
    
    
    """
    def __init__(self, daq_xml_config , data_queue, task_queue, notif_queue):
        multiprocessing.Process.__init__(self)
        self.data_queue = data_queue
        self.task_queue = task_queue
        self.notify_queue = notif_queue
        self.go_on = True
        self.daq_running = False
        self.data_archives = []

        self._hdf_threads = HDFWTHREADS
        #: The number of bunches to be saved for each file. This parameter
        #: may be changed afterward and will affect the next HDF5 file to 
        #: be created. The Default Value is 15.
        self.file_size = HDF5FILESIZE

        #: an instance of :class:`~fermidaq.lib.metadatahandler.MetaDataHandler`
        #: responsible for managing the metadata. 
        try:
            self.meta = MetaDataHandler(daq_xml_config)
        except:
            self.meta = None

        #: an instance of :class:`~fermidaq.lib.xmlconfig.DaqXmlConfig` - has all the 
        #: information necessary to know how to create the 
        #: :class:`~fermidaq.lib.attribdaq.AttribDaq` and how to organize them inside
        #: the HDF5 file.
        #: 
        #: List of :class:`~fermidaq.lib.bunchmanager.BunchManager.HDFs`.
        #: 
        self._hdfwriters = self._init_hdfs('', '')
        self.file_prefix = ''
        self.Files_contiguous = True
        self.files_opened = 0
        self._stop_at_current_file = False
        self.allocated_bunch_range = (0, 0)
        self.files2save = -1
        self.first_shot_saved = 0
        self.shots_saved = 0
        self._daq_list = dict()
        self.mutex = threading.Lock()


    class HDFs():
        """
        Auxiliar class to help controlling the :class:`~fermidaq.lib.hdfwriter.HDFWriter`
        threads. It has the following attributes: 
        
        * ptr2hdfwriter: instance of :class:`~fermidaq.lib.hdfwriter.HDFWriter`
        * initial_bn   : the first bunch number for saving file for that instance.
        * last_bn      : the last bunch number to save in the HDF5 file for that instance.
        * working      : flag to indicate if it is busy or idle.
        """
        def __init__(self,pt):
            self.ptr2hdfwriter = pt
            self.hdf_key = pt.key
            self.initial_bn = 0
            self.last_bn = 0
            self.working = False
            
    def run(self):
        try:
            if self.meta is not None:
                print "Meta starting"
                self.meta.start()

            [th.ptr2hdfwriter.start() for th in self._hdfwriters]
            
            while self.go_on:
                self._process_tasks()
                if not self.daq_running:
                    time.sleep(0.01)
                    continue
                dataready = True
                read_loop = 0
                while self.data_queue.qsize() > 0 and read_loop < 20000: #not self.data_queue.empty() and read_loop < 20:
                    data_in = self.data_queue.get()
                    self.store_data(data_in)
                    read_loop += 1
                    self.data_queue.task_done()
                # Sort archive on the base of bunchnumber
                self.data_archives.sort(key=self.getKey)
                #
                store_loop = 0
                while len(self.data_archives) > 0 and store_loop < 10000:
                    # Wait for slow data
                    next_daq_key = (self.data_archives[0])[0]
                    next_bn_in = (self.data_archives[0])[1]
                    next_bn_fi = (self.data_archives[0])[2]
                    self.mutex.acquire()
                    self.data_ready(next_daq_key,next_bn_in,next_bn_fi)
                    self.mutex.release()
                    store_loop += 1
        except:
            print traceback.format_exc()
        #
        for _hdf in self._hdfwriters:
            _hdf.ptr2hdfwriter.stop_thread()

    
    def _process_tasks(self):
        while self.task_queue.qsize() > 0:
            try:
                next_task = self.task_queue.get()
                print next_task
                if next_task[0] == 'stop':
                    if self.meta is not None:
                        self.meta.go_on = False
                    self.go_on = False
                elif next_task[0] == 'stop_and_clear':
                    for _hdf in self._hdfwriters:
                        _hdf.ptr2hdfwriter._force_close_daq()
                    while not self.data_queue.empty():
                        self.data_queue.get()
                    del self.data_archives[:]
                    if self.meta is not None:
                        # Stop metadata acquisition
                        self.meta.stop_polling()
                    self.daq_running = False
                    #
                elif next_task[0] == 'file_prefix':
                    self.file_prefix = next_task[1]
                    for _hdf in self._hdfwriters:
                        _hdf.ptr2hdfwriter.file_prefix = next_task[1]
                elif next_task[0] == 'file_path':
                    self.file_path = next_task[1]
                    for _hdf in self._hdfwriters:
                        _hdf.ptr2hdfwriter.file_path = next_task[1]
                elif next_task[0] == 'Files_contiguous':
                    self.Files_contiguous = next_task[1]
                elif next_task[0] == 'stop_at_this_file':
                    self._stop_at_current_file = True
                elif next_task[0] == 'Files_to_save':
                    self.files2save = next_task[1]
                elif next_task[0] == 'File_size':
                    self.file_size = next_task[1]
                elif next_task[0] == 'daq_switch_off':
                    for _hdf in self._hdfwriters:
                        _hdf.ptr2hdfwriter.daq_switch_off(next_task[1])
                    for daq_key in next_task[1]:
                        if daq_key in self._daq_list.keys():
                            self._daq_list.pop(daq_key)
                elif next_task[0] == 'daq_switch_on':
                    for daq_key in next_task[1]:
                        self._daq_list[daq_key] = 0
                elif next_task[0] == 'start_daq':
                    self.daq_running = True
                    self._stop_at_current_file = False
                    self.files_saved = 0
                    self.files_opened = 0
                    self.allocated_bunch_range = (0,0)
                    self.first_shot_saved = 0
                    self.shots_saved = 0
                    if self.meta is not None:
                        self.meta.start_polling()
                elif next_task[0] == 'pause_metadata':
                    daq_meta_key = next_task[1]
                    do_pause = next_task[2]
                    self.meta.meta_attribute[daq_meta_key].is_paused = do_pause
                elif next_task[0] == 'hdf_finished':
                    hdf_key = next_task[1]
                    full_file_path = next_task[2]
                    report = next_task[3]
                    self.hdf_finished(hdf_key, full_file_path, report)
                #
                self.task_queue.task_done()
            except:
                print traceback.format_exc()

        

                
    def getKey(self,dataitem):
        # return bn_in for sorting the list
        return dataitem[1]

    def store_data(self,data_in):
        try:
            daq_key = data_in[0]
            bn_in = min(data_in[1],data_in[2])
            bn_fi = max(data_in[1],data_in[2])
            if daq_key not in self._daq_list.keys():
                print "ABORRRO!",daq_key
                self._daq_list[daq_key] = 0
                #return
            if isinstance(data_in[3], list) or isinstance(data_in[3], numpy.ndarray):
                #self.data_ready(daq_key,bn_in,bn_fi,data_in[3])
                self.data_archives.append((daq_key,bn_in,bn_fi,data_in[3]))
                if len(data_in[3]) != (bn_fi - bn_in + 1):
                    print "MMMMMMM.....",daq_key,bn_in,bn_fi,len(data_in[3])
        except:
            print traceback.format_exc()
            

    def data_ready(self,daq_key,bn_in,bn_f):
        """
        Receive Notification from :class:`~fermidaq.lib.attribdaq.AttribDaq`
        and pass them to the :class:`~fermidaq.lib.hdfwriter.HDFWriter`. 
        
        In order to do this, it will first, look for the list of the
        busy :class:`~fermidaq.lib.hdfwriter.HDFWriter` so, to see, if 
        one of those threads must be notified. 
    
        If none of those threads should be notified, it will pick one of 
        the idle threads, and pass to it the notification, just after configuring
        its acquisition range (:meth:`~fermidaq.lib.hdfwriter.HDFWriter.save_conf`).

        """
        try:
            # first of all: we must be sure that there are hdfs allocated
            # for the given range:
            # so, if the last allocated bunch number is lower
            # than the current given final bunch number, we should allocate
            # more hdfs to store this data.
            while (self.Files_contiguous or self.files_opened < 1) and \
                    not self._stop_at_current_file and \
                    self.allocated_bunch_range[1] < bn_f  \
                    and (self.files_opened < self.files2save 
                    or self.files2save == -1): # it will allocate up to files2save 
                    #at this moment we do not accept that both, bn_in and bn_f is
                    #so great that we would have to allocate more than one hdfs
                    #at this single time.
                    #assert 
                    #bn_f - self.allocated_bunch_range[1] < self.file_size
                    #new range:
                    if (not self.Files_contiguous) or (self.allocated_bunch_range[0] < 0):
                        all_bn_in = bn_in
                    else:
                        all_bn_in = self.allocated_bunch_range[1] + 1
                    all_bn_f = all_bn_in + self.file_size - 1

                    idle_hdfwriter = [hdf for hdf in self._hdfwriters 
                                        if hdf.working == False]

                    #check if there is a free hdfwriter
                    if len(idle_hdfwriter) == 0:
                        if len(self._hdfwriters) < MAX_HDFWTHREADS:
                            new_id = len(self._hdfwriters)
                            fpath = self.file_path
                            fpref = self.file_prefix
                            #
                            key='hdf%d'%(new_id)
                            self._hdfwriters += [self.HDFs(HDFWriter(key, self,
                                                                     file_path=fpath, file_prefix=fpref))]
                            #
                            self._hdfwriters[-1].ptr2hdfwriter.start()
                            idle_hdfwriter = [self._hdfwriters[-1]]
                        else:
                            # NO MORE HDFs!
                            break
        
                    if len(idle_hdfwriter) > 0:
                        #get the pointer to the free hdfwriter.
                        free_hdfwriter = idle_hdfwriter[0]
        
                        #add one new hdfsPyTango.DevState.ON
                        if _dbg:
                            print ("""DataServer: Allocating hdfwriter %s 
                                for range %d->%d"""% (free_hdfwriter.hdf_key, 
                                                      all_bn_in, all_bn_f))
                        assert (all_bn_f - all_bn_in + 1) == self.file_size
                        free_hdfwriter.ptr2hdfwriter.file_path = self.file_path
                        free_hdfwriter.ptr2hdfwriter.file_prefix = self.file_prefix
                        free_hdfwriter.ptr2hdfwriter.save_conf(all_bn_in, all_bn_f,self.Files_contiguous)
                        free_hdfwriter.initial_bn = all_bn_in
                        free_hdfwriter.last_bn = all_bn_f
                        free_hdfwriter.ptr2hdfwriter.daq_switch_on(self._daq_list.keys())
                        free_hdfwriter.working = True
        
                        if (self.allocated_bunch_range[0] <= 0):
                            self.allocated_bunch_range = (all_bn_in, all_bn_f)
                        else:
                            self.allocated_bunch_range = (min(all_bn_in,self.allocated_bunch_range[0]),max(all_bn_f,self.allocated_bunch_range[1]))
                        print "self.data_archives",len(self.data_archives),self.data_queue.qsize()
                        self.files_opened += 1
            #
            # Extract data from internal data archive
            data_in = (self.data_archives.pop(0))[3]
            #
            if (bn_f > self.allocated_bunch_range[1]):
                if (bn_in > self.allocated_bunch_range[1]):
                    # chunk of data cannot be allocated at the moment, skip.
                    self.data_archives.append((daq_key, bn_in, bn_f,data_in))
                    return
                # not all shots can be saved (no more HDF threads)
                # postpone 'overflow' shots
                last_avail_bn = self.allocated_bunch_range[1]
                self.data_archives.append((daq_key, last_avail_bn+1, bn_f,data_in[-(bn_f-last_avail_bn):]))
                if len(data_in[-(bn_f-last_avail_bn):]) != (bn_f- (last_avail_bn + 1) + 1):
                    print "UUUUUUU.....",daq_key,last_avail_bn+1,bn_f,len(data_in[-(bn_f-last_avail_bn):])
                #
                data_in = data_in[:-(bn_f-last_avail_bn)]
                bn_f = last_avail_bn
                if len(data_in) != (bn_f-bn_in+1):
                    print "********",daq_key,len(data_in),(bn_f-bn_in),bn_in,bn_f
            #
            if (bn_in < self.allocated_bunch_range[0]):
                # purge too old data
                if (bn_f < self.allocated_bunch_range[0]):
                    # chunk of data too old: forget about it
                    return
                data_in = data_in[-(bn_f-self.allocated_bunch_range[0]+1):]
                bn_in = self.allocated_bunch_range[0]
                if len(data_in) != (bn_f-bn_in+1):
                    print "#########",daq_key,len(data_in),(bn_f-bn_in),bn_in,bn_f
            #
            #
            # Look for all working hdfs which of them has the initial
            # bunch and the final bunch in the range of the received
            # bunches.
            # So, if the initial bunch of the hdf writer is greater of
            # the last bunch received or if the last bunch of the hdf writer
            # is lower than the first bunch received, this means that the
            # range of this hdfwriter is outside the range of this
            # input and must be rejected.
            # NOTE: working_entry[1] initial_bunch
            #       working_entry[2] final_bunch
            #
            #  The rule is reject the hdf if:
            #     hdf.initial_bunch > bn_f or hdf.final_bunch < bn_in
            #
            pass2this_working = None
            pass2this_working = [working_entry for working_entry in \
                         self._hdfwriters \
                         if working_entry.working == True and \
                         not (working_entry.initial_bn > bn_f \
                         or working_entry.last_bn < bn_in)]

            daq_bn_f = -1
            daq_bn_in = -1
            last_bn_saved = -1
            for hdfs_entry in pass2this_working:
                hdf_key = hdfs_entry.hdf_key
                hdf_bn_in = hdfs_entry.initial_bn
                hdf_bn_f = hdfs_entry.last_bn
                daq_bn_in = max(hdf_bn_in, bn_in)
                daq_bn_f = min(hdf_bn_f, bn_f)
                idx_in = daq_bn_in - bn_in
                idx_f = daq_bn_f - bn_in + 1
                last_bn_saved = max(last_bn_saved,daq_bn_f)
                hdfs_entry.ptr2hdfwriter.save_data_list(daq_key, daq_bn_in, daq_bn_f,data_in[idx_in:idx_f])
            
            if (daq_bn_f != -1) and (daq_bn_in != -1):
                if (self.Files_contiguous):
                    if (self.first_shot_saved == 0):
                        self.first_shot_saved = daq_bn_in
                    new_shots_saved = daq_bn_f - self.first_shot_saved + 1
                    self.shots_saved = max(self.shots_saved,new_shots_saved)
                elif len(pass2this_working):
                    # only one file opened at time
                    new_shots_saved = (self.files_saved * self.file_size) + daq_bn_f - pass2this_working[0].initial_bn + 1
                    self.shots_saved = max(self.shots_saved, new_shots_saved)
            else:
                # Nothing has been done put 
                self.data_archives.append((daq_key,bn_in,bn_f,data_in))
                if len(data_in) != (bn_f - bn_in + 1):
                    print "XXXXXXXXX.....",daq_key,bn_in,bn_f,len(data_in)

        except:
            print traceback.format_exc()
            pass

    def hdf_finished(self, hdf_key, full_file_path, report):
        """
        Receive the notification from :class:`~fermidaq.lib.hdfwriter.HDFWriter`
        that it has concluded a file acquisition. 
        
        After receiving this notificatio, it removes the 
        :class:`~fermidaq.lib.hdfwriter.HDFWriter`
        from the busy thread, making it available to receive a new request for 
        saving a new HDF5 file.
        
        :param hdf_pt: Instance of :class:`~fermidaq.lib.hdfwriter.HDFWriter` that has concluded an HDF5 file. 
        :param full_file_path: Path of the file just concluded.        
                
        """
        self.mutex.acquire()
        try:
            # Set hdfwriter as not working and update allocated bunch range info
            
            min_bn_in = 0
            for hdf in self._hdfwriters:
                if hdf.hdf_key == hdf_key:
                    hdf.working = False
                else:
                    if (min_bn_in == 0) or (hdf.initial_bn < min_bn_in):
                        min_bn_in = hdf.initial_bn
                
            self.allocated_bunch_range = (min_bn_in,self.allocated_bunch_range[1])
            self.notify_queue.put(['hdf_finished',hdf_key,full_file_path,report])

            self.files_saved += 1
            if not self.Files_contiguous:
                self.files_opened -= 1
        except:
            print traceback.format_exc()
        self.mutex.release()


    def _init_hdfs(self, file_path, file_prefix):
        """  Create a dictionary for be managed by the idle_hdfwriter

        :returns: A list of :class:`~fermidaq.lib.bunchmanager.BunchManager.HDFs` for management of :class:`~fermidaq.lib.hdfwriter.HDFWriter` threads.
        """
        self.file_path = file_path
        self.file_prefix = file_prefix
        self.files_saved = 0

        d = []
        for i in range(self._hdf_threads):
            key ='hdf%d'%(i)
            d += [self.HDFs(HDFWriter(key,self,file_path=file_path,file_prefix=file_prefix))]
        return d

