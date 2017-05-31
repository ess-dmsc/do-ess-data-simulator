import zmq
import traceback
import socket
import time

class CommunicationClass:
    def __init__(self, name='director'):
        self.context = zmq.Context()
        self.poller = zmq.Poller()
        self.pub_sock = None
        self.sub_socks = {}
        self.pub_tag = name
        #
        self.create_pub_socket()



#-----------------------------------------------------------------------------------
#    create_pub_socket:
#		
#-----------------------------------------------------------------------------------
    def create_pub_socket(self):
        try:
            self.pub_sock = self.context.socket(zmq.PUB)
            self.pub_port = self.pub_sock.bind_to_random_port("tcp://0.0.0.0")
            print "PUB " + "tcp://" + str(self.pub_port)
        except:
            traceback.print_exc()
            self.pub_sock = None
       
#-----------------------------------------------------------------------------------
#    create_sub_socket:
#		
#-----------------------------------------------------------------------------------
    def create_sub_socket(self, name, url):
        try:
            if name in self.sub_socks:
                self.poller.unregister(self.sub_socks[name])
                self.sub_socks[name].close()
            self.sub_socks[name] = self.context.socket(zmq.SUB)
            self.sub_socks[name].setsockopt(zmq.SUBSCRIBE, '')
            self.sub_socks[name].connect("tcp://"+str(url))
            self.poller.register(self.sub_socks[name], zmq.POLLIN)
            #print "SUB TO " + "tcp://" + str(url),self.sub_socks[name]
        except:
            traceback.print_exc()
            print "tcp://"+str(url)
            del self.sub_socks[name]
            return False
        return True

#-----------------------------------------------------------------------------------
#    my_pub_socket_info :
#		
#-----------------------------------------------------------------------------------
    def my_pub_socket_info(self):
        return socket.gethostname()+":"+str(self.pub_port)
       
#-----------------------------------------------------------------------------------
#    publish_ack :
#		
#-----------------------------------------------------------------------------------
    def publish_ack(self, ack_tag, trg_start, trg_stop):
        # At the moment just use send_pyobj	
        self.pub_sock.send_pyobj([ack_tag, trg_start,trg_stop])


#-----------------------------------------------------------------------------------
#    publish_data :
#		
#-----------------------------------------------------------------------------------
    def publish_data(self, tag, trg_start, trg_stop, data_value):
        # At the moment just use send_pyobj
        self.pub_sock.send_pyobj(['data',tag.lower(), trg_start,trg_stop,data_value])


#-----------------------------------------------------------------------------------
#    publish_info :
#		
#-----------------------------------------------------------------------------------
    def publish_info( self, priority = -1, data_names=[]):
        # At the moment just use send_pyobj
        self.pub_sock.send_pyobj(['info',{'prio':priority,'data':data_names}])


#-----------------------------------------------------------------------------------
#    ask_for_info :
#		
#-----------------------------------------------------------------------------------
    def ask_for_info(self, srv_name, timeout_sec=1):
        # At the moment just use send_pyobj
        self.pub_sock.send_pyobj(["info", srv_name])
        msg = []
        sub_socket = self.sub_socks[srv_name]
        max_retries = 5
        retry = 0
        while retry < max_retries and msg == []:
            socks = dict(self.poller.poll((1000./max_retries)*timeout_sec))
            #if len(socks) == 0:
            #    return msg
            if sub_socket in socks and socks[sub_socket] == zmq.POLLIN:
                try:
                    reply = sub_socket.recv_pyobj()
                    if reply[0] == 'info':
                        msg = reply[1]
                except:
                    traceback.print_exc()
                    msg = []
            retry += 1
        return msg


#-----------------------------------------------------------------------------------
#    ask_for_log :
#		
#-----------------------------------------------------------------------------------
    def ask_for_log(self, srv_name, timeout_sec=1):
        # At the moment just use send_pyobj
        self.pub_sock.send_pyobj(["playerlog", srv_name])
        msg = []
        sub_socket = self.sub_socks[srv_name]
        max_retries = 5
        retry = 0
        while retry < max_retries and msg == []:
            socks = dict(self.poller.poll((1000./max_retries)*timeout_sec))
            #if len(socks) == 0:
            #    return msg
            if sub_socket in socks and socks[sub_socket] == zmq.POLLIN:
                try:
                    reply = sub_socket.recv_pyobj()
                    if reply[0] == 'data' and reply[1] == 'playerlog':
                        msg = reply[4]
                except:
                    traceback.print_exc()
                    msg = []
            retry += 1
        return msg


#-----------------------------------------------------------------------------------
#    wait_message :
#		
#-----------------------------------------------------------------------------------
    def wait_message(self, srv_names, timeout_sec=1):
        try:
            msg = {}
            socks = dict(self.poller.poll(1000*timeout_sec))
            if len(socks) == 0:
                return msg
            for sn in srv_names:
                s = self.sub_socks[sn]
                if s in socks and socks[s] == zmq.POLLIN:
                    recv_msg = s.recv_pyobj()
                    msg[sn] = recv_msg
        except:
            traceback.print_exc()
            msg = None
        return msg

#-----------------------------------------------------------------------------------
#    publish_command :
#		
#-----------------------------------------------------------------------------------
    def publish_command(self, command, srv_name, argin=None, timeout_sec=1):
        # At the moment just use send_pyobj	
        self.pub_sock.send_pyobj([command, srv_name, argin])
        print "Sent command:", command, srv_name, argin
        msg = []        
        sub_socket = self.sub_socks[srv_name]        
        max_retries = 5
        retry = 0        
        while retry < max_retries and msg == []:
            socks = dict(self.poller.poll((1000./max_retries)*timeout_sec))
            if sub_socket in socks and socks[sub_socket] == zmq.POLLIN:
                try:
                    reply = sub_socket.recv_pyobj()
                    if reply[0] == command and reply[1] == reply[2] == -1:
                        return True
                except:
                    traceback.print_exc()
                    return False
            retry += 1
        return False


#-----------------------------------------------------------------------------------
#    publish_trigger :
#		
#-----------------------------------------------------------------------------------
    def publish_trigger(self, trigger_value, priority):
        # At the moment just use send_pyobj	
        self.pub_sock.send_pyobj(["trigger", trigger_value, priority])

