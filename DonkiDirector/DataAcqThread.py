import time
import threading 
import zmq
import traceback

THREAD_DELAY_SEC = 0.05
SHOTS_BUFFER_SIZE = 100
'''
class DataEventCallBack(PyTango.utils.EventCallBack):
    def __init__(self, parent_obj):
        self.parent = parent_obj
	return
  
    def push_event(self, event_data):
        try:
	    if not event_data.err:
		player_name = "/".join(event_data.attr_name.split("/")[-4:-1])
		ack_value = event_data.attr_value.value
		ack_trg = event_data.attr_value.time.tv_sec
		ack_attrname = event_data.attr_name.split("/")[-1]
		if abs(ack_trg - self.parent.last_ack) > 10000:
			# Fake trigger
			return
		self.parent.last_ack = ack_trg
		self.parent.last_ack_time = time.time()
		self.parent._data_buffer[ack_trg] = ack_value
		if self.parent._data_buffer.has_key(ack_trg-SHOTS_BUFFER_SIZE):
			del self.parent._data_buffer[ack_trg-SHOTS_BUFFER_SIZE]
		#print "DAQ THREAD",player_name,ack_trg,ack_value
		if (ack_trg == 0) and (self.parent.main_thread.max_triggers != 1):
			# Postpone the first trigger, just to be sure that the sequence really started
			return
		elif (ack_trg == 1) and (self.parent.main_thread.max_triggers != 1):
			# Notify main thread also the first data arrived (trg == 0)
			self.parent.main_thread.notify_new_data(self.parent, 0)	
		# Notify main thread that new data arrived
		self.parent.main_thread.notify_new_data(self.parent, ack_trg)	
        except Exception, e:
	    print e
'''


class DataAcqThread(threading.Thread):

#-----------------------------------------------------------------------------------
#    __init__
#-----------------------------------------------------------------------------------
    def __init__(self, parent_obj, zmq_pub_url, data_name, alias_name):
        threading.Thread.__init__(self)
        self._alive = True
        self._started = False
        self.main_thread = parent_obj
        self.player_pub_url = zmq_pub_url
        self.data_name = data_name.lower()
        self.data_alias = alias_name
        #
        self.context = zmq.Context()
        self.poller = zmq.Poller()
        self.sub_sock = None
        self._data_buffer = {}
        self.last_ack = -1
        self.last_ack_time = time.time()
        self.myState = "STANDBY"
        #
        self.set_new_url(zmq_pub_url)


#-----------------------------------------------------------------------------------
#    set_new_url
#-----------------------------------------------------------------------------------
    def set_new_url(self, url):
        try:
            if self.sub_sock is not None:
                self.poller.unregister(self.sub_sock)
                self.sub_sock.close()
            self.sub_sock = self.context.socket(zmq.SUB)
            self.sub_sock.setsockopt(zmq.SUBSCRIBE, '')
            self.sub_sock.connect("tcp://"+str(url))
            self.poller.register(self.sub_sock, zmq.POLLIN)
        except:
            traceback.print_exc()
            del self.sub_sock
            self.sub_sock = None

#-----------------------------------------------------------------------------------
#    run
#-----------------------------------------------------------------------------------
    def run(self):
        self.myState = "STANDBY"
        while self._alive:
            if self._started:
                self.myState = "ON"
                self.last_ack_time = time.time()
                while (self._started):
                    socks = dict(self.poller.poll(1000))
                    if len(socks) > 0 and self.sub_sock in socks and socks[self.sub_sock] == zmq.POLLIN:
                        try:
                            reply = self.sub_sock.recv_pyobj()
                            if reply[0] == 'data' and (reply[1]).lower() == self.data_name:
                                self.main_thread.notify_new_data(self.data_alias,reply[2],reply[3],reply[4])
                        except:
                            traceback.print_exc()
                    if (time.time() - self.last_ack_time) > 20 :
                        self.myState = "OFF"
                    elif (self.myState != "ON") :
                        self.myState = "ON"
            self.myState = "STANDBY"
            time.sleep(THREAD_DELAY_SEC)

