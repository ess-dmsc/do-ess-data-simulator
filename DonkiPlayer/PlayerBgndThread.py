import time
import threading
import os,sys 
from circularBuffer import circularBuffer
import socket
import DonkiOrchestraLib
import traceback
import numpy

THREAD_DELAY_SEC = 0.1
DATABUFFERSIZE = 100
DEBUG = True

class processThread(threading.Thread):

#-----------------------------------------------------------------------------------
#    __init__
#-----------------------------------------------------------------------------------
    def __init__(self, pub_tag, info_server = 'localhost:50010', action_code = 'scripts/tangotest_daq.py'):
        threading.Thread.__init__(self)
        self._alive = True
        self._started = False
        self._state = 'STANDBY'
        self.seq_values = []
        self.trg_idx = 0
        self.Director = None
        self.trigger_event_id = 0
        self.databuffers = {}
        self.seq_values = []
        self.datalock = threading.Lock()
        self.director_url = ''
        #
        self.info_server = info_server
        self.pub_tag = pub_tag.lower()
        self.ActionCode = action_code
        self.prio = 0
        self.lastlog = ''
        #
        if len(self.ActionCode) and self.ActionCode.strip().endswith('.py'):
            scriptfile = self.ActionCode.strip()
            if os.path.isfile(scriptfile):
                f = open(scriptfile)
                content = f.readlines()
                f.close()
                self.code_module = self.importCode('\n'.join(content),"action_code")
            else:
                self._state = 'ALARM'
        else:
            self.code_module = self.importCode('\n'.join(self.dev.ActionCode),"action_code")
        #
        if not hasattr(self.code_module,'action') or not callable(getattr(self.code_module,'action')):
            self._state = 'ALARM'
        else:
            try:
                self.data_names = self.code_module.get_data_names() 
            except:
                self.data_names = []
        #
        self.onlyAck = (self.data_names == [])
        if not self.onlyAck:
            for dname in self.data_names:
                self.databuffers[dname] = circularBuffer(DATABUFFERSIZE)
        #
        self.zcc = DonkiOrchestraLib.CommunicationClass(self.pub_tag)


#-----------------------------------------------------------------------------------
#    exec_action :	
#			
#-----------------------------------------------------------------------------------
    def exec_action(self, trg):
        if DEBUG:
            t0 = time.time()
        try:
            retval = self.do_stuff()
            #
            if not self.onlyAck and retval != None:
                for resname in retval:
                    if resname in self.data_names:
                        self.datalock.acquire()
                        self.databuffers[resname.lower()].push(trg, retval[resname])
                        self.datalock.release()
                    elif resname.lower() == 'playerlog':
                        self.lastlog = time.asctime() + " " + retval[resname]
                        if DEBUG:
                            print self.lastlog
                        continue
                    if isinstance(retval[resname], list):
                        self.push_data(resname.lower(),trg,trg,retval[resname])
                    elif isinstance(retval[resname], numpy.ndarray):
                        if (retval[resname]).ndim == 1:
                            resdata = (retval[resname]).reshape(1,(retval[resname]).shape[0])
                        elif (retval[resname]).ndim == 2:
                            resdata = (retval[resname]).reshape(1,(retval[resname]).shape[0],(retval[resname]).shape[1])
                        self.push_data(resname.lower(),trg,trg,resdata)
                    else:
                        self.push_data(resname.lower(),trg,trg,[retval[resname]])
            #
            self.zcc.publish_ack('ack',trg,trg)
        except Exception, e:
            if DEBUG:
                traceback.print_exc()
                self.push_data('playerlog',trg,trg,str(e))
        if DEBUG:
            print "Delay:",(time.time()-t0) * 1000,"ms"


#-----------------------------------------------------------------------------------
#    update_connection_info
#-----------------------------------------------------------------------------------
    def update_connections(self):
        info_host = self.info_server.split(":")[0]
        info_port = int(self.info_server.split(":")[1])
        
        try:
            info_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            info_sock.connect((info_host, info_port))
            # Retrieve director URL
            msg = "get donkidirector\r\n"
            info_sock.send(msg)
            reply = info_sock.recv(2048)
            if reply == '':
                raise RuntimeError("socket connection broken")
            dd_url = eval(reply)[0]['data']
            if dd_url != self.director_url:
                self.zcc.sub_socks.clear()
                self.zcc.create_sub_socket('donkidirector',dd_url)
                # Send my PUB URL
                msg = "set donkiplayers "+self.zcc.pub_tag+" "+self.zcc.my_pub_socket_info()+"\r\n"
                print msg
                info_sock.send(str(msg))
                reply = info_sock.recv(2048)
                info_sock.close()
                if reply == '':
                    raise RuntimeError("socket connection broken")
                self.director_url = dd_url
        except Exception,e:
            if 'Connection refused' in str(e):
                print "Information server not responding (%s:%d)" % (info_host, info_port)
            else:
                traceback.print_exc()
            return False
        return True
            


#-----------------------------------------------------------------------------------
#    push_data :	
#			
#-----------------------------------------------------------------------------------
    def push_data(self, data_name, trg_start, trg_stop, data_value):
            self.zcc.publish_data(data_name,trg_start,trg_stop,data_value)


#-----------------------------------------------------------------------------------
#    get_data :	- data_name: name of the data attribute
#		- params: [0,N] read last N samples, [1,bn_start,bn_stop] read from bn_start to bn_stop
#-----------------------------------------------------------------------------------
    def get_data(self, data_name, params):
        #
        if data_name not in self.databuffers:
            raise RuntimeError("Wrong data_name")
            #
            self.datalock.acquire()
            #
        if (params[0] == 0) and (len(params) > 1):
            data = self.databuffers[data_name].get_last_data(params[1])
        elif (params[0] == 1) and (len(params) > 2):
            data = self.databuffers[data_name].get_data_chunk(params[1],params[2])
        else:
            self.datalock.release()
            raise RuntimeError("Wrong command parameters")
        #
        self.datalock.release()
        if len(data) == 0:
            raise RuntimeError("Requested data unavailable")
        return data

#-----------------------------------------------------------------------------------
#    importCode
#-----------------------------------------------------------------------------------
    def importCode(self, code,name,add_to_sys_modules=0):
	"""
	Import dynamically generated code as a module. code is the
	object containing the code (a string, a file handle or an
	actual compiled code object, same types as accepted by an
	exec statement). The name is the name to give to the module,
	and the final argument says wheter to add it to sys.modules
	or not. If it is added, a subsequent import statement using
	name will return this module. If it is not added to sys.modules
	import will try to load it in the normal fashion.

	import foo

	is equivalent to

	foofile = open("/path/to/foo.py")
	foo = importCode(foofile,"foo",1)

	Returns a newly generated module.
	"""
	import sys,imp

	module = imp.new_module(name)

	exec code in module.__dict__
	if add_to_sys_modules:
		sys.modules[name] = module

	return module


#-----------------------------------------------------------------------------------
#    do_stuff
#-----------------------------------------------------------------------------------
    def do_stuff(self):
	if self._state != 'ON':
		return
	#
	trg = self.last_trg
	if len(self.seq_values) == 0:
		retval = self.code_module.action(trg)
	elif self.trg_idx < len(self.seq_values):
		retval = self.code_module.action(trg,self.seq_values[self.trg_idx])
		self.trg_idx += 1
		if self.trg_idx >= len(self.seq_values):
			self._state = 'STANDBY'
	return retval


#-----------------------------------------------------------------------------------
#    do_start
#-----------------------------------------------------------------------------------
    def do_start(self):
	if self._state != 'ON':
		return
	if not hasattr(self.code_module,'setup') or not callable(getattr(self.code_module,'setup')):
		return
	try:
         ret = self.code_module.setup()
         if ret != None:
             self.lastlog = str(ret)
             if DEBUG:
                 print self.lastlog
	except:
         traceback.print_exc()
	

#-----------------------------------------------------------------------------------
#    do_stop
#-----------------------------------------------------------------------------------
    def do_stop(self):
	if not hasattr(self.code_module,'stop') or not callable(getattr(self.code_module,'stop')):
		return
	try:
         ret = self.code_module.stop()
         if ret != None:
             self.lastlog = str(ret)
             if DEBUG:
                 print self.lastlog
	except:
         traceback.print_exc()




#-----------------------------------------------------------------------------------
#    run
#-----------------------------------------------------------------------------------
    def run(self):
        #
        self.update_connections()
        #
        last_msg_time = time.time()
        while self._alive:
            if not self._started:
                # Check for new messages from Director
                dd_msg = self.zcc.wait_message(['donkidirector'])
                try:
                    if 'donkidirector' in dd_msg:
                        last_msg_time = time.time()
                        new_msg = dd_msg['donkidirector']
                        topic = new_msg[0].lower()
                        if topic == 'info' and new_msg[1] == self.pub_tag:
                            time.sleep(0.1)
                            self.zcc.publish_info(self.prio,self.data_names)
                        elif topic == 'playerlog' and new_msg[1] == self.pub_tag:
                            time.sleep(0.1)
                            self.zcc.publish_data('playerlog',-1, -1, self.lastlog)
                        elif topic == 'start' and new_msg[1] == self.pub_tag:
                            self._started = True
                        elif topic == 'priority' and new_msg[1] == self.pub_tag:
                            self.prio = new_msg[2]
                            self.zcc.publish_ack('priority',-1,-1)
                        elif topic == 'stop' and new_msg[1] == self.pub_tag:
                            self.do_stop()
                            self.zcc.publish_ack(topic,-1,-1)
                        elif topic == 'trigger':
                            trg = new_msg[1]
                            prio = new_msg[2]
                            if (self.prio >= 0) and ((trg != -1) or (prio != -1)):
                                print "IT'S NOT A DIRECTOR PING TRIGGER!"
                            self.zcc.publish_ack('ack',-1,-1)
                    #
                    if time.time() - last_msg_time > 5:
                        # In the last 5 seconds we did not receive any message...
                        # check director URL
                        self.update_connections()
                        last_msg_time = time.time()
                except:
                    traceback.print_exc()
            else:
                self.trg_idx = 0
                self._state = 'ON'
                self.last_trg = 0
                self.do_start()
                # Acknowledge Director that we started
                self.zcc.publish_ack('start',-1,-1)
                while (self._started) and (self._state == 'ON'):
                    try:
                        # Check for new messages from Director
                        dd_msg = self.zcc.wait_message(['donkidirector'])
                        if 'donkidirector' in dd_msg:
                            last_msg_time = time.time()
                            new_msg = dd_msg['donkidirector']
                            topic = new_msg[0].lower()
                            if topic == 'trigger':
                                trg = new_msg[1]
                                prio = new_msg[2]
                                if prio == self.prio and self.last_trg == (trg - 1):
                                    self.last_trg = trg
                                    self.exec_action(trg)
                            elif topic == 'stop':
                                name = str(new_msg[1]).lower()
                                if name == self.pub_tag:
                                    self.do_stop()
                                    # Acknowledge Director that we've stopped
                                    self.zcc.publish_ack(topic,-1,-1)
                                    self._started = False
                            elif topic == 'start':
                                name = str(new_msg[1]).lower()
                                if name == self.pub_tag:
                                    self.do_start()
                                    # Acknowledge Director that we started
                                    self.zcc.publish_ack(topic,-1,-1)
                            elif topic == 'playerlog' and new_msg[1] == self.pub_tag:
                                time.sleep(0.1)
                                self.zcc.publish_data('playerlog',-1, -1, self.lastlog)
                            else:
                                print "MAH!",new_msg
                        #
                        if time.time() - last_msg_time > 5:
                            # In the last 5 seconds we did not receive any message...
                            # check director URL
                            if not self.update_connections():
                                self._started = False
                            last_msg_time = time.time()
                    except:
                        if DEBUG:
                            traceback.print_exc()
                        continue
                self._started = False
                self._state = 'STANDBY'


