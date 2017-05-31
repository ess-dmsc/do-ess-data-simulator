import time
import threading 
import os
from DataAcqThread import DataAcqThread
import zmq
from DonkiOrchestraLib import CommunicationClass
import traceback
import socket
import multiprocessing
from InfoServer import infoServerThread
from DataServer import DataServer

THREAD_DELAY_SEC = 1

DEBUG = False


class directorThread(threading.Thread):

#-----------------------------------------------------------------------------------
#    __init__
#-----------------------------------------------------------------------------------
    def __init__(self,tango_dev):
        threading.Thread.__init__(self)
        self._alive = True
        self._started = False
        self._paused = False
        self.dev = tango_dev
        self.actual_priority = 0
        self.PlayersInfo = {}
        self.DataAliases = {}
        self.Players_per_level = {}
        self.busy_Players = []
        self.trg = -1
        self.max_triggers = 10
        self._state = 'STANDBY'
        self.EnableDataSaving = False
        self.Report = ''
        self.daq_threads = {}
        #
        self.zcc = CommunicationClass()
        #
        self.sub_socks = {}
        self.sub_ports = {}
        self.infoServer = infoServerThread(Port=50010, notif_function = self.info_db_changed)
        #
        director_tokens = ['donkidirector','director',self.zcc.my_pub_socket_info()]
        self.infoServer.write_to_db(director_tokens)
        #
        for da in eval(self.infoServer.get_from_db(['dataaliases'])):
            self.DataAliases[str(da['name'])] =  str(da['data'])
        #
        self.infoServer.start()
        #
        daq_xml_config = "test.xml"
        #
        self.dataserver_task_queue = multiprocessing.JoinableQueue()
        self.dataserver_data_queue = multiprocessing.JoinableQueue() #(maxsize=50)
        self.dataserver_notif_queue = multiprocessing.JoinableQueue()
        self.dataserver = DataServer(daq_xml_config,
                                     data_queue=self.dataserver_data_queue,
                                     task_queue=self.dataserver_task_queue,
                                     notif_queue=self.dataserver_notif_queue)
        self.dataserver.start()
        # set default properties about file saving
        self.set_file_prefix('daqfile')
        self.set_files_contiguous(True)
        self.set_files_to_save(1)
        self.set_file_size(100)
        self.set_file_path(".")

        


		
		
#-----------------------------------------------------------------------------------
#    players_changed
#-----------------------------------------------------------------------------------
    def info_db_changed(self, tablename, new_info):
        if str(tablename) == 'donkidirector':
            return
        elif str(tablename) == 'donkiplayers':
            if DEBUG:
                print "players changed",new_info
            else:
                print "players changed",new_info
            try:
                self.PlayersInfo.clear()
                for pl in new_info:
                    self.PlayersInfo[str(pl['name'])] = {'url':str(pl['data'])}
            except:
                traceback.print_exc()
        elif str(tablename) == 'dataaliases':
            if DEBUG:
                print "data aliases changed",new_info
            else:
                print "data aliases changed"
            try:
                self.DataAliases.clear()
                for da in new_info:
                    self.DataAliases[da['name']] =  str(da['data'])
            except:
                traceback.print_exc()
            


#-----------------------------------------------------------------------------------
#    retrieve_players_info
#-----------------------------------------------------------------------------------
    def retrieve_players_info(self, reconnect = False):
        #
        not_active_players = []
        max_priority = -1
        self.Players_per_level.clear()
        self.Players_per_level[0] = []
        try:
            for pl_key in self.PlayersInfo.keys():
                pl_name = str(pl_key)
                if reconnect and (not self.zcc.create_sub_socket(pl_name,self.PlayersInfo[pl_name]['url'])):
                    continue
                print "Asking info to", pl_name
                info = self.zcc.ask_for_info(pl_name)
                print info
                if len(info) == 0:
                    not_active_players.append(pl_name)
                    continue
                if info['data'] == []:
                    self.PlayersInfo[pl_name]['type'] ='ack'
                else:
                    self.PlayersInfo[pl_name]['type'] ='data'
                    zmq_pub_url = self.PlayersInfo[pl_name]['url']
                    for dataname in info['data']:
                        daq_th_name = pl_name+"/"+dataname.lower()
                        if daq_th_name not in self.DataAliases:
                            self.infoServer.write_to_db(['dataaliases',daq_th_name,daq_th_name])
                        data_alias = self.DataAliases[daq_th_name]
                        if daq_th_name not in self.daq_threads:
                            self.daq_threads[daq_th_name] = DataAcqThread(self, zmq_pub_url, dataname, data_alias)
                            self.daq_threads[daq_th_name].start()
                        if (self.daq_threads[daq_th_name].player_pub_url != zmq_pub_url):
                            # Player ZMQ PUB URL has changed
                            self.daq_threads[daq_th_name].set_new_url(zmq_pub_url)
                        if (self.daq_threads[daq_th_name].data_alias != data_alias):
                            # Player Data alias has changed
                            self.daq_threads[daq_th_name].data_alias = data_alias
                dprio = info['prio']
                if dprio < 0:
                    self.PlayersInfo[pl_name]['prio'] = "Disabled"
                else:
                    self.PlayersInfo[pl_name]['prio'] = str(dprio)
                if dprio > max_priority:
				max_priority = dprio
                if dprio not in self.Players_per_level.keys():	
				self.Players_per_level[dprio] = []
                if not pl_name in self.Players_per_level[dprio]:
				self.Players_per_level[dprio].append(pl_name)
            #
            for pl_name in not_active_players:
                self.PlayersInfo.pop(pl_name)
        except:
            traceback.print_exc()
        return max_priority


#-----------------------------------------------------------------------------------
#    start_stop_Players
#-----------------------------------------------------------------------------------
    def start_stop_Players(self, bool_in):
        max_priority = 0
        #
        for a in self.PlayersInfo.keys():
            if self.PlayersInfo[a]['prio'] == "Disabled":
                continue
            try:
                if (bool_in):
                    ok = self.zcc.publish_command('start',a)
                else:
                    ok = self.zcc.publish_command('stop',a)
                print "Reply", ok
            except:
                if DEBUG:
                    traceback.print_exc()
        for k in self.daq_threads:
            self.daq_threads[k]._started = bool_in
        if bool_in:
            self.dataserver_task_queue.put(['start_daq'])
            self.dataserver_task_queue.join()
        return
        for k in self.MetadataSources:
            if self.MetadataSources[k]['enabled']:
                if bool_in:
				self.MetadataSources[k]['status'] = 'ON'
                elif self.MetadataSources[k]['status'] != 'ALARM':
				self.MetadataSources[k]['status'] = 'STANDBY'
	
#-----------------------------------------------------------------------------------
#    set_player_priority
#-----------------------------------------------------------------------------------
    def set_player_priority(self, player_name, priority):
        print "Set Priority"
        try:
            ok = self.zcc.publish_command('priority', player_name, priority)
            if ok:
                self.PlayersInfo[player_name]['prio'] = str(priority)
                print "Ok"
                print self.PlayersInfo
            else:
                print "Error: player did not reply"
        except:
            if DEBUG:
                traceback.print_exc()

#-----------------------------------------------------------------------------------
#    notify_new_data
#-----------------------------------------------------------------------------------
    def notify_new_data(self, data_name, trg_in, trg_f, data_in):
        print "NEW DATA",data_name,trg_in, trg_f
        if self._state != 'ON':
            return
        if abs(trg_in - self.trg) > 1000:
            # Fake trigger value
            return
        if not self.EnableDataSaving:
            return
        #elif not self.PlayersInfo[daq_thread.player_nickname]['enabled']:
        #    return
        if DEBUG:
            print "NEW DATA",data_name,trg_in, trg_f
        self.dataserver_data_queue.put([data_name,trg_in,trg_f,data_in])


#-----------------------------------------------------------------------------------
#    report_message
#-----------------------------------------------------------------------------------
    def report_message(self, message_in, with_date = False):
	if with_date:
		message_in = time.asctime() + " " + message_in
	new_report = ("\n".join([self.Report,message_in])).split("\n")
	self.Report = "\n".join(new_report[-10000:])
	

#-----------------------------------------------------------------------------------
#    ResetReport
#-----------------------------------------------------------------------------------
    def ResetReport(self):
        self.Report = ""


#-----------------------------------------------------------------------------------
#    set_DataAlias
#-----------------------------------------------------------------------------------
    def set_DataAlias(self, player_data_name, alias_name):
        self.infoServer.write_to_db(['dataaliases',player_data_name,alias_name])

#-----------------------------------------------------------------------------------
#    set_file_prefix
#-----------------------------------------------------------------------------------
    def set_file_prefix(self, prefix):
        self.dataserver_task_queue.put(['file_prefix',prefix])
        self.dataserver_task_queue.join()
        self.file_prefix = prefix

#-----------------------------------------------------------------------------------
#    set_file_path
#-----------------------------------------------------------------------------------
    def set_file_path(self, fpath):
        self.dataserver_task_queue.put(['file_path',fpath])
        self.dataserver_task_queue.join()
        self.file_path = fpath

#-----------------------------------------------------------------------------------
#    set_files_contiguous
#-----------------------------------------------------------------------------------
    def set_files_contiguous(self, bool_in):
        self.dataserver_task_queue.put(['Files_contiguous',bool_in])
        self.dataserver_task_queue.join()
        self.files_contiguous = bool_in

#-----------------------------------------------------------------------------------
#    set_files_to_save
#-----------------------------------------------------------------------------------
    def set_files_to_save(self, nFiles):
        self.dataserver_task_queue.put(['Files_to_save',nFiles])
        self.dataserver_task_queue.join()
        self.files_to_save = nFiles

#-----------------------------------------------------------------------------------
#    set_file_size
#-----------------------------------------------------------------------------------
    def set_file_size(self, nTriggers):
        self.dataserver_task_queue.put(['File_size',nTriggers])
        self.dataserver_task_queue.join()
        self.file_size = nTriggers

#-----------------------------------------------------------------------------------
#    get_file_prefix
#-----------------------------------------------------------------------------------
    def get_file_prefix(self):
        return self.file_prefix

#-----------------------------------------------------------------------------------
#    get_file_path
#-----------------------------------------------------------------------------------
    def get_file_path(self):
        return self.file_path

#-----------------------------------------------------------------------------------
#    get_files_contiguous
#-----------------------------------------------------------------------------------
    def get_files_contiguous(self):
        return self.files_contiguous

#-----------------------------------------------------------------------------------
#    get_files_to_save
#-----------------------------------------------------------------------------------
    def get_files_to_save(self):
        return self.files_to_save

#-----------------------------------------------------------------------------------
#    get_file_size
#-----------------------------------------------------------------------------------
    def get_file_size(self):
        return self.file_size

#-----------------------------------------------------------------------------------
#    quit_and_exit
#-----------------------------------------------------------------------------------
    def quit_and_exit(self):
        self._alive = False


#-----------------------------------------------------------------------------------
#    run
#-----------------------------------------------------------------------------------
    def run(self):
        knownPlayersInfo = self.PlayersInfo.copy()
        while self._alive:
            if not self._started:
                if knownPlayersInfo != self.PlayersInfo:
                    self.retrieve_players_info(reconnect = True)
                    knownPlayersInfo = self.PlayersInfo.copy()
                else:
                    # Send a dummy negative trigger, something like a 'ping'
                    self.zcc.publish_trigger(-1, -1)
                    not_responding_Players = self.PlayersInfo.keys()
                    t0 = time.time()
                    while not_responding_Players and not self._started:
                        pl_msgs = self.zcc.wait_message(not_responding_Players)
                        if pl_msgs is not None and len(pl_msgs):
                            for pl in pl_msgs:
                                idx = not_responding_Players.index(pl)
                                del not_responding_Players[idx]
                        elif (time.time() - t0) > 5:
                            print "NOT RESPONDING",not_responding_Players
                            knownPlayersInfo = None
                            break
            else:
                upper_priority = self.retrieve_players_info()
                self.start_stop_Players(True)
                self.ResetReport()
                self._state = "ON"
                self.report_message("DonkiDirector started")
                self._paused = False
                self.trg = 1
                while ((self.trg <= self.max_triggers) or (self.max_triggers < 0)):
                    if not self._started :
                        break
                    if (self._paused):
                        self._state = "STANDBY"
                        time.sleep(0.1)
                        continue
                    self._state = "ON"
                    for priority in range(upper_priority+1):
                        if not priority in self.Players_per_level.keys():
                            continue
                        self.busy_Players= self.Players_per_level[priority][:]
                        if DEBUG:
                            print "----","TRIGGER:",self.trg,"PRIORITY:",priority,"----"
                        self.actual_priority = priority
                        t0 = time.time()
                        #self.dev.push_event ("Trigger",[], [], priority, self.trg,PyTango._PyTango.AttrQuality.ATTR_VALID)
                        self.zcc.publish_trigger(self.trg, priority)
                        while self.busy_Players and self._started:
                            pl_msgs = self.zcc.wait_message(self.busy_Players)
                            if len(pl_msgs):
                                for pl in pl_msgs:
                                    new_msg = pl_msgs[pl]
                                    topic = new_msg[0].lower()
                                    trg = new_msg[1]
                                    prio = new_msg[2]
                                    if topic == 'ack' and trg == self.trg:
                                        idx = self.busy_Players.index(pl)
                                        del self.busy_Players[idx]
                            elif (time.time() - t0) > 60:
                                self.zcc.publish_trigger(self.trg, priority)
                                #self.dev.push_event ("Trigger",[], [], priority, self.trg,PyTango._PyTango.AttrQuality.ATTR_VALID)
                                t0 = time.time()
                        if DEBUG:
                            print "Delay:",(time.time()-t0) * 1000,"ms"
                    self.trg += 1
                self.start_stop_Players(False)
                self._state = "OFF"
                self.report_message("DonkiDirector stopped")
                self._started = False
            time.sleep(THREAD_DELAY_SEC)
        #
        self.infoServer._stop_ = True
        #
        self.dataserver_task_queue.put(['stop'])
        #
        for dtkey in self.daq_threads.keys():
            self.daq_threads[dtkey]._started = False
            self.daq_threads[dtkey]._alive = False
	    

