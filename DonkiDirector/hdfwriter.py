# -*- coding: utf-8 -*-
"""
The main class inside the module :mod:`~fermidaq.lib.hdfwriter` is the
:class:`~fermidaq.lib.hdfwriter.HDFWriter` that it the resposible for
producing the HDF5 files from the data acquired from :mod:`~fermidaq.lib.attribdaq`.

In order to parallelize the production of the HDF5 files (see  :ref:`daq_architecture`), the :class:`~fermidaq.lib.hdfwriter.HDFWriter` classes are threads. So, it will be possible to configure one or more threads to cope with the performance requirements.

Some tests were done, :mod:`~fermidaq.test.bunchmanagertest`, and we were able to
collect Images 2160x2600 @10Hz using no more than 3 threads.


Look at the main attributes and methods from  :class:`~fermidaq.lib.hdfwriter.HDFWriter`:

.. image:: /images/hdfwriter_uml.png

Attributes :attr:`~fermidaq.lib.hdfwriter.HDFWriter.file_path` and
:attr:`~fermidaq.lib.hdfwriter.HDFWriter.file_prefix` controls the name and
path of the HDF5 file to be created.

:attr:`~fermidaq.lib.hdfwriter.HDFWriter.key` is an auxiliar attribute that
helps debbuging the system with an human readable name for the thread.

The methods :meth:`~fermidaq.lib.hdfwriter.HDFWriter.daq_switch_on` and
:meth:`~fermidaq.lib.hdfwriter.HDFWriter.daq_switch_off` help the :class:`~fermidaq.lib.hdfwriter.HDFWriter` to know 'a priori' which are the data it
should expect to receive. For performance reason, it is advizable to
inform these threads if it should not expect data from an specific
:class:`~fermidaq.lib.attribdaq.AttribDaq` to be received. This information
is very usefull to be able to know when a file may be considered concluded. And the thread will be able to notify the  :class:`~fermidaq.lib.bunchmanager.BunchManager` that it may receive more request to save data (:meth:`~fermidaq.lib.hdfwriter.HDFWriter._file_concluded`).


Finally, the main methods are :meth:`~fermidaq.lib.hdfwriter.HDFWriter.save_conf` that informs this thread about what are the range of bunch numbers it should expected to save the data. While  :meth:`~fermidaq.lib.hdfwriter.HDFWriter.save_data_list` will pass the reference it needs to get the data from
:class:`~fermidaq.lib.attribdaq.AttribDaq` through its method :class:`~fermidaq.lib.attribdaq.AttribDaq.get_data`.

.. seealso::

   For examples on how to configure and use :mod:`~fermidaq.lib.hdfwriter`,
   see :mod:`~fermidaq.test.bunchmanagertest`.


HDFWriter
=========

.. autoclass:: fermidaq.lib.hdfwriter.HDFWriter (bunch_manager, key, file_path, file_prefix [, logger = MyLogger()])
    :members:

Protected Members:
------------------

.. automethod:: fermidaq.lib.hdfwriter.HDFWriter._file_concluded

.. automethod:: fermidaq.lib.hdfwriter.HDFWriter._close_data_acquisition

.. automethod:: fermidaq.lib.hdfwriter.HDFWriter._save_hdf


Exceptions
==========

.. autoexception:: fermidaq.lib.hdfwriter.HDFWriterBusy

.. autoexception:: fermidaq.lib.hdfwriter.HDFWriterNotStarted


"""
import time
import numpy
import threading
import h5py
import traceback
import os
from Queue import (Queue, Full, Empty)

LOGFILTER = 10

def ensure_dir(f):
    d = os.path.dirname(f)
    if not os.path.exists(d):
        os.makedirs(d)


class HDFWriterBusy(Exception):
    """
    Notify that :class:`~fermidaq.lib.hdfwriter.HDFWriter` is not
    able to process another request.
    """
    pass

class HDFWriterNotStarted(Exception):
    """
    Notify that the thread :class:`~fermidaq.lib.hdfwriter.HDFWriter`
    was not started.
    """
    pass

HDFW_INIT = 0
HDFW_IDLE = 1
HDFW_BUSY = 2

class HDFWriter(threading.Thread):
    """
    Responsible for collecting the data from the
    :class:`~fermidaq.lib.attribdaq.AttribDaq` objects and placing them
    in HDF5 files.

    They are generated and controlled by
    :class:`~fermidaq.lib.bunchmanager.BunchManager`. The constructor requires:

    :param bunch_manager: The instance of the :class:`~fermidaq.lib.bunchmanager.BunchManager` object.
    :param           key: A string to identify this thread.
    :param     file_path: The file system path where to save the produced files.
    :param   file_prefix: The initial part of the HDF5 file that will be created.

    Optionally it may receive:

    :param logger: An instance of Tango Server logger in order to produce loggings inside the Tango logging system.


    The way it will produce HDF5 files are:

    * It must receive the range of bunches to save :meth:`~fermidaq.lib.hdfwriter.HDFWriter.save_conf`
    * Them it will receive all the data to save through :meth:`~fermidaq.lib.hdfwriter.HDFWriter.save_data_list`.
    * After finishing the acquisition of all the data to produce the file it will notify the :class:`~fermidaq.lib.bunchmanager.BunchManager` through its method :meth:`~fermidaq.lib.bunchmanager.BunchManger.hdf_finished` in order to announce that it is free to acquire a another file.


    Internally, it will manage a Queue of data to save entries that
    will be dealt with inside its main thread execution (:meth:`~fermidaq.lib.hdfwriter.HDFWriter.run`).

    The attributes :attr:`~fermidaq.lib.hdfwriter.HDFWriter.file_path` and
    :attr:`~fermidaq.lib.hdfwriter.HDFWriter.file_prefix` may be changed
    and they will affect the next file to be produced.

    .. todo::

        Change the notification method to add the full path name of the file.
        Provide a notificatio service for problems inside this thread, to notify BunchManager.

    """
    def __init__(self, key, dataserver, file_path, file_prefix):
        threading.Thread.__init__(self)
        #public:

        #: identifies this thread in a human way
        self.key = key
        #
        self.dataserver = dataserver
        #: define where the file will be created.
        self.file_path=file_path
        #: define the beggining of the file, the
        #: full name of the file will be:
        #: file_path + file_prefix + first_bunch_nubmer + .h5
        self.file_prefix = file_prefix
        

        #this list is filled through the save_data_list method, and
        #consumed inside the main thread, specially inside
        #the _save_hdf method.
        self._save_list = Queue(-1) # Original version => Queue(100)

        #indicates id there is an opened file being filled.
        self._hdf_file = None

        #flag to indicate that thread should close.
        self._stop = False
        #dictionary that deals with the FermiDaq to acquire, it
        #is used mainly to allow the HDFWriter to know if it has acquired all
        #the necessary data.
        self._daq_list = dict()
        #timeouts_count
        self._intState = HDFW_INIT
        self.first_bunch = 0
        self.last_bunch = 0
        self.qt = 0
        self.dsets = []
        self.dsets_ptr = {}
        self._log_filter = 0
        self.report = ""
        self.Files_contiguous = True

    def save_data_list(self, daq_key, bn_in, bn_fin,data_in):
        """
        Called from :class:`~fermidaq.lib.bunchmanager.BunchManager` in order
        to inform the list of data available.

        It will fulfill an internal queue that will be dealt inside the
        main thread - :meth:`~fermidaq.lib.hdfwriter.HDFWriter.run`.

        This queue may be full, due, for example, for the fact that this
        thread is not able to put to file all the data in the time it
        should do. In this case, this method will raise an
        :exc:`~fermidaq.FermiDaqSystem.FatalInternalError`. At this
        exception, the :class:`~fermidaq.lib.bunchmanager.BunchManager`
        should abort the excecution of this thread.

        :param daq_pt: instance of :class:`~fermidaq.lib.attribdaq.AttribDaq`
        :param  bn_in: first bunch number to ask for.
        :param bn_fin: last bunch number to ask for.

        With this entry, the :class:`~fermidaq.lib.hdfwriter.HDFWriter`
        assumes that it may call the
        :meth:`~fermidaq.lib.attribdaq.AttribDaq.get_data` ::

            daq_pt.get_data(bn_in, bn_fin)

        And it will receive the data to produce the HDF5 file.

        """
        try:
            #assert self._intState == HDFW_BUSY
            assert bn_in >= self.first_bunch
            assert bn_fin <= self.last_bunch
        except AssertionError, ex:
            print traceback.format_exc()
            return
        self._log_filter += 1
        if not self._log_filter % LOGFILTER:
            self._log_filter = 0
            print ("HDFW %s daq = %s %d->%d" %(self.key,daq_key,bn_in,bn_fin))
        try:
            #insert inside the save_list
            self._save_list.put((daq_key, bn_in, bn_fin,data_in),timeout=1)
        except Full:
            raise FatalInternalError("Queue is full, thread is not working")

    def daq_switch_on(self,daq_pt_list):
        """
        Initialize the entries for the expected acquisition daqs.

        :param daq_pt_list: List of instances of :class:`~fermidaq.lib.attribdaq.AttribDaq` or a single :class:`~fermidaq.lib.attribdaq.AttribDaq`.

        The :class:`~fermidaq.lib.hdfwriter.HDFWriter` will wait for all
        entries in this list to provide the data in the range defined at
        :meth:`~fermidaq.lib.hdfwriter.HDFWriter.save_conf` before closing
        the file.
        """
        if isinstance(daq_pt_list, list):
            for daq_key in daq_pt_list:
                self._daq_list[daq_key] = (0,0)
        else:
            print("HDFW %s daq_switch_on(), unknown input argument")


    def daq_switch_off(self,daq_pt_list):
        """
        Removes from its internal management of acquired data, those
        entries that should not produce data anymore.

        It should be called by :class:`~fermidaq.lib.bunchmanager.BunchManager`
        in order to inform that :class:`~fermidaq.lib.hdfwriter.HDFWriter`
        should not expect to receive data from those
        :class:`~fermidaq.lib.attribdaq.AttribDaq` any more.

        :param daq_pt_list: list of :class:`~fermidaq.lib.attribdaq.AttribDaq` or a single :class:`~fermidaq.lib.attribdaq.AttribDaq`.

        .. warning::

            Not being notified that it should not expect data from one
            :class:`~fermidaq.lib.attribdaq.AttribDaq` that does not produce
            data will damage the performance of the
            :class:`~fermidaq.lib.hdfwriter.HDFWriter`, because it will not
            be able to know exactly when it should close the file.

        """
        if isinstance(daq_pt_list,list):
            for daq_key in daq_pt_list:
                if daq_key in self._daq_list.keys():
                    self._daq_list.pop(daq_key)
        else:
            print("HDFW %s daq_switch_off(), unknown input argument")



    def save_conf(self,bn_in, bn_fin, Files_contiguous):
        """
        Define the range of the HDF5 file to be saved.

        This method is called from
        :class:`~fermidaq.lib.bunchmanager.BunchManager` and define the range
        of bunch numbers the :class:`~fermidaq.lib.hdfwriter.HDFWriter`
        should expect to produce the HDF5 file.

        For example::

            hdf_writer.save_conf( 5, 105)

        Will inform the :class:`~fermidaq.lib.hdfwriter.HDFWriter` that it
        should create an HDF5 file and that that file will save the data
        from bunch number 5 till bunch number 105. So, it will be able
        to know when the acquisition has finished in order to notify
        the :class:`~fermidaq.lib.bunchmanager.BunchManager`.

        :param bn_in: The first bunch number it will save in HDF5 file.
        :param bn_fin: The last bunch number for the file.

        .. warning::

            :class:`~fermidaq.lib.hdfwriter.HDFWriter` does not accept a new
            configuration if it is acquiring data for the current file.
            If it occurrs, it will raise the exception
            :exc:`~fermidaq.lib.hdfwriter.HDFWriterBusy`.

        .. warning::

            :class:`~fermidaq.lib.hdfwriter.HDFWriter` is a thread, and
            there is no sense to configure an acquisition if the thread
            is not alive. So, it will raise the
            :exc:`~fermidaq.lib.hdfwriter.HDFWriterNotStarted` exception
            if the thread is not alive.

        """
        if self._intState == HDFW_BUSY:
            raise HDFWriterBusy("HDFW %s is already configured for %d->%d"
            %(self.key,self.first_bunch,self.last_bunch))
        if self._intState == HDFW_INIT:
            raise HDFWriterNotStarted("HDFW %s is not started"%(
                                self.key))

        print("HDFW %s config file for %d->%d"%(
                                self.key, bn_in,bn_fin))
        self.first_bunch = bn_in
        self.last_bunch = bn_fin
        self.qt = bn_fin - bn_in + 1
        self.Files_contiguous = Files_contiguous
        self._intState = HDFW_BUSY
        



    def stop_thread(self):
        """ Ask thread to finish itself.

        It is an assynchronous method, and the correct way to ensure
        the thread finishes it self is through::

            hdf_writer.start()

            #do its jobs...

            hdf_writer.stop_thread()
            hdf_writer.join()


        """
        self._stop = True

    def run(self):
        """
        The main function of the :class:`~fermidaq.lib.hdfwriter.HDFWriter`.

        It 'consumes' its internal queue with the entries of data
        to save that were populated by the
        :meth:`~fermidaq.lib.hdfwriter.HDFWriter.save_data_list`.
        With the data acquired, it produces the HDF5 files through
        the :meth:`~fermidaq.lib.hdfwriter.HDFWriter._save_hdf`.

        If the queue is empty - there is no data for saving in the queue -
        it will investigate if there is no more data to acquire
        (:meth:`~fermidaq.lib.hdfwriter.HDFWriter._file_concluded`).

        If it has received the whole expected data, it will close
        the file and notify the
        :class:`~fermidaq.lib.bunchmanager.BunchManager`
        (:meth:`~fermidaq.lib.hdfwriter.hdfwriter._close_data_acquisition`)

        """
        print("HDFW %s started"%(self.key))
        self._intState = HDFW_IDLE
        self.last_savetime = time.time()

        while not self._stop:
            # Check tasks to perform
            try:
                #receive the new entry
                if self.Files_contiguous:
                    TIMEOUT_SEC = 10
                else:
                    TIMEOUT_SEC = 10
                if self._save_list.qsize() > 0:
                    try:
                        (daq_key, bn_in, bn_fin, data_in) = self._save_list.get()
                        if isinstance(data_in, list):
                            self._save_hdf(daq_key,bn_in, bn_fin, numpy.asarray(data_in))
                        else:
                            self._save_hdf(daq_key,bn_in, bn_fin, data_in)
                        self.last_savetime = time.time()
                    except:
                        print traceback.format_exc()
                elif self._file_concluded():
                    self._close_data_acquisition(timeout=False)
                elif time.time() - self.last_savetime > TIMEOUT_SEC and self._hdf_file:
                    self._close_data_acquisition(timeout=True)

            except:
                #THIS MEANS THAT LESS THEN 3 VALUES WERE PASSED,
                #THIS IS THE STOP REQUEST.
                print traceback.format_exc()
                print('HDFW %s received STOP Request' % self.key)
                self.report += "Received STOP Request" + "\n"

        self._close_data_acquisition()
        print("HDFW %s thread finished"%(self.key))


    def _file_concluded(self):
        """ Returns True if there is one file just filled, that should
        be closed.

        The difficulty to know if the file has finished is that the
        :class:`~fermidaq.lib.hdfwriter.HDFWriter`
        may data from :class:`~fermidaq.lib.attribdaq.AttribDaq` in a 'random'
        order. This, method, tries to ensure that it has acquired all the
        bunch ranges from all the :class:`~fermidaq.lib.attribdaq.AttribDaq`
        it should receive data from.


        .. warning::

            If there is one :class:`~fermidaq.lib.attribdaq.AttribDaq` from
            whom the :class:`~fermidaq.lib.hdfwriter.HDFWriter` does not
            receive new data for at least 3 seconds after another
            :class:`~fermidaq.lib.attribdaq.AttribDaq` reaches the last
            bunch number to acquire, it will assumes that
            this :class:`~fermidaq.lib.attribdaq.AttribDaq` is not working
            correctly and it will remove that entry from the expected
            data to receive.

        .. todo::

            It should expect at least one
            :class:`~fermidaq.lib.attribdaq.AttribDaq` to reach the
            last bunch number before assuming that one of the
            :class:`~fermidaq.lib.attribdaq.AttribDaq` may have problem.

        """

        # it there is not open file, it does not finished.
        if not self._hdf_file:
            return False
        elif self._intState == HDFW_IDLE:
            return True
            
        #get all the daqattrib that has reached the last number
        l= [(key,value[1]) for (key,value) in self._daq_list.items()
                    if value[1] == self.last_bunch ]
                        
        # if all the values reached the last value, the file concluded
        return len(l) == len(self._daq_list.keys())

    def _force_close_daq(self):
        while not self._save_list.empty():
            self._save_list.get()
        self._intState = HDFW_IDLE

    def _close_data_acquisition(self,timeout=False):
        """
        Finishes the HDF5 file and notify the
        :class:`~fermidaq.lib.bunchmanager.BunchManager` that it is free to
        receive new request.

        .. todo::

            It should add here the metadata information.
        """
        if self._hdf_file == None:
            return
        print("HDFW finished key = %s [%d,%d]"% (
                    self.key,self.first_bunch,self.last_bunch))
        self._intState = HDFW_IDLE
        try:
            self._hdf_file.create_dataset('bunches',
                data=numpy.arange(self.first_bunch,self.last_bunch+1),
                             dtype=numpy.int32)
        except:
            print("Failed to create the bunches dataset!" )
            self.report += "Error: " + "failed to create the bunchnumber dataset!" + "\n"

        for daq_key in self.dsets:
            try:
                if daq_key in self.dataserver.meta.meta:
                    meta_attrs = {}
                    attrib_metadata = self.dataserver.meta.meta[daq_key]
                    for (key,meta_entry) in attrib_metadata:
                        meta_value = meta_entry.value
                        if meta_value == None:
                            print "Error: " + daq_key + " metadata readout failed, check device "+ meta_entry.entry_name + "\n"
                            meta_value = ""
                        meta_attrs[key] = meta_value
                    if len(meta_attrs) == 0:
                        continue
                    dset = self._hdf_file[daq_key]                    
                    for metakey in meta_attrs:
                        dset.attrs[metakey] = meta_attrs[metakey]
            except:
                self.report += "Error: " + daq_key + " metadata readout failed.\n"
                print traceback.format_exc()

        #Adding the metadata attributes values
        try:
            meta_attrs={}
            for key,meta_attr in self.dataserver.meta.meta_attribute.iteritems():
                if not meta_attr.is_paused:
                    meta_attrs[key] = meta_attr.value
            for metakey in meta_attrs:
                if meta_attrs[metakey] != None:
                    self._hdf_file.create_dataset(metakey, data=meta_attrs[metakey])
        except :
            self.report += "Error: metadata attributes readout failed.\n"
            print traceback.format_exc()

        try:
            self.dsets = []
            file_name = self._hdf_file.filename
            tt0 = time.time()
            self._hdf_file.flush()
            self._hdf_file.close()
            print "FILE",file_name," closed",1000.*(time.time() - tt0)        
            self._hdf_file = None
            self.report += file_name.split("/")[-1] + " Closed.\n"
            print("_close_data_acquisition DONE!")
            #if at least one reached the last bunch:
            if timeout:
                #get all the daqattrib that has reached the last number
                not_finished_l= [(key,value[1]) for (key,value)
                            in self._daq_list.items()
                            if value[1] != self.last_bunch ]
                #
                print self.key,"TIMEOUT",self.last_bunch,not_finished_l
            self._daq_list.clear()
            self.dataserver.hdf_finished(self.key,file_name,self.report)
            #self.dataserver.task_queue.put(['hdf_finished',self.key,file_name,self.report])
        except:
            print traceback.format_exc()

    def _save_hdf(self, daq_key, bn_in, bn_fin, data_in):
        """
        Save data from :class:`~fermidaq.lib.attribdaq.AttribDaq` in HDF5.

        Usually, the :class:`~fermidaq.lib.hdfwriter.HDFWriter` does not
        know the kind of data it will receive from the
        :class:`~fermidaq.lib.attribdaq.AttribDaq`.

        The algorithm for saving HDF5 file is the following:

        * Ensure that it already knows the :class:`~fermidaq.lib.attribdaq.AttribDaq` that is producing this data, and keep track of the bunches that it has acquired from this Daq.

        * Ensure it has already opened a file for this new acquisition.

        * Acquire the data.

        * Check if there is an HDF5 dataset configured for this data.

            If not, it will check the
            :attr:`~fermidaq.lib.attribdaq.AttribDaq.attr_type` to know
            if it should create a scalar, spectrum or image dataset.
            From the data itself, it will get the data type, and also
            deduce the shape of the dataset to create.

        * Update the dataset

        * Check if the acquisition is done for the current configured range - :meth:`~fermidaq.lib.hdfwriter.HDFWriter.save_conf`, eventually, conclude the acquisition.

        """
        self._log_filter += 1
        if not self._log_filter % LOGFILTER or True:
            print("HDFW %s saving data %s %d %d"% (
                        self.key, daq_key,bn_in,bn_fin))

        #keep track of the daq and the bunches already acquired.
        dic_entry = self._daq_list.get(daq_key,None)
        if not dic_entry:
            self._daq_list[daq_key] = ( bn_in, bn_fin)
        else:
            self._daq_list[daq_key] = ( bn_in, bn_fin)

        self.report = ""
        #open a new file if there is no file opened.
        if self._hdf_file == None:
            if self.file_path[-1] != '/':
                self.file_path += '/'
            f_name = "%s%s_%s.h5"% (self.file_path , self.file_prefix,
                                str(self.first_bunch))
            print("HDFW %s opening file %s"% (
                            self.key, f_name))
            ensure_dir(f_name)
            self._hdf_file = h5py.File(f_name,'w')
            #self._hdf_file = h5py.File(f_name,'w',libver='latest')
            self._hdf_file.attrs['timestamp'] = str(time.ctime())
            self.dataserver.notify_queue.put(['update_report',f_name.split("/")[-1] + " Opened.\n"])
            self.last_savetime = time.time()

        #check if its data set was already configured.
        if daq_key not in self.dsets:
            tokens = daq_key.split("/")
            groupname=""
            dsetname = tokens[-1]
            for idx in range(len(tokens)-1):
                groupname += "/"
                groupname += tokens[idx]
                try:
                    g=self._hdf_file.create_group(groupname)
                except:
                    #print "****", groupname,"NOT CREATED"
                    # Probably the grooup already exists... does not matter
                    pass

            #
            try:
                if (groupname != ""):
                    g = self._hdf_file[groupname]
                    if len(data_in.shape) == 1: # 'scalar':
                        g.create_dataset(dsetname,shape=(self.qt,),dtype=data_in.dtype)
                    elif len(data_in.shape) == 2: #'spectrum'
                        g.create_dataset(dsetname,shape=(self.qt, data_in.shape[-1]),dtype=data_in.dtype)
                    elif len(data_in.shape) == 3: #'image'
                        g.create_dataset(dsetname,shape=(self.qt, data_in.shape[-2], data_in.shape[-1]),dtype=data_in.dtype)
                else:
                    if len(data_in.shape) == 1: # 'scalar':
                        self._hdf_file.create_dataset(daq_key,shape=(self.qt,),dtype=data_in.dtype)
                    elif len(data_in.shape) == 2: #'spectrum'
                        self._hdf_file.create_dataset(daq_key,shape=(self.qt,data_in.shape[-1]),dtype=data_in.dtype)
                    elif len(data_in.shape) == 3: #'image'
                        self._hdf_file.create_dataset(daq_key,shape=(self.qt, data_in.shape[-2], data_in.shape[-1]),dtype=data_in.dtype)
                #
                self.dsets += [daq_key]
                self._hdf_file.flush()
                self.dsets_ptr[daq_key] = self._hdf_file.get(daq_key, None)
                #attempt to solve the problem of Bug#2
            except ValueError:
                print('file %s data_set %s already exist'%(self._hdf_file.filename, daq_key))
                self.report += "Error: dataset " + daq_key + " already exist." + "\n"
            except Exception, ex:
                print traceback.format_exc()
                print('HDFW %s file %s data_set %s creation error %s' %(self.key, self._hdf_file.filename, daq_key,str(ex)))
                self.report += "Error: dataset " + daq_key + " creation error." + "\n"

        try:
            #update the dataset
            assert self.dsets_ptr[daq_key]
            try:
                slicc = slice(bn_in - self.first_bunch, bn_fin - self.first_bunch + 1)
                self.dsets_ptr[daq_key][slicc] = data_in
            except Exception, ex:
                print traceback.format_exc()
                print daq_key,slicc,self.first_bunch,bn_in,bn_fin,self._hdf_file.filename
                self.report += "Error: dataset " + daq_key + " write error." + "\n"
            #check if the file is finished.
            if self._file_concluded():
                #print ' save hdf close data acquisition'
                print self.key,"CONCLUDED!"
                print('HDFW %s file concluded' % self.key)
                self._close_data_acquisition()
        except ValueError, ex:
            self.report += "Error: dataset " + daq_key + " h5py.get[dataset] failed." + "\n"
            #Bug#3, found that sometimes it is not able to get the correct dataset
            print("HDFW %s dataset %s h5py.get[dataset] failed %s" %
                (self.key, daq_key, str(ex)))
        except AssertionError, ex:
            #Bug#3
            self.report += "Error: dataset " + daq_key + " assertion error." + "\n"
            print("HDFW %s dataset %s assertion error" %
                    (self.key, daq_key))
