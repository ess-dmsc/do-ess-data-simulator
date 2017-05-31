#!/usr/bin/env python
import sys
import os
import time
import threading
import thread
import signal
from socket import *
import types
from tinydb import TinyDB, Query, where
import traceback
import select

#
class infoServerThread(threading.Thread):

#-----------------------------------------------------------------------------------
#    __init__
#-----------------------------------------------------------------------------------
    def __init__(self, Port, tinydb_file_path="./db.json", notif_function = None):
        threading.Thread.__init__(self)
        self.Port = Port
        self._stop_ = False
        self.BUFFSIZE = 50000
        self.notif_function = notif_function
        self.mutex = threading.Lock()
        self.db = TinyDB(tinydb_file_path)	

#-----------------------------------------------------------------------------------
#    __del__
#-----------------------------------------------------------------------------------
    def __del__(self):
        self._stop_ = True

#-----------------------------------------------------------------------------------
#    run
#-----------------------------------------------------------------------------------
    def run(self):
        #
        ADDR = ("", self.Port)
        serversock = socket(AF_INET, SOCK_STREAM)
        serversock.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
        serversock.bind(ADDR)
        serversock.listen(5)
        read_list = [serversock]
        while not self._stop_ :
            readable, writable, errored = select.select(read_list, [], [], 3)
            for s in readable:
                if s is serversock:
                    clientsock, addr = serversock.accept()
                    thread.start_new_thread(self.handler, (clientsock, addr))


#-----------------------------------------------------------------------------------
#    handler
#-----------------------------------------------------------------------------------
    def handler(self,clientsock,addr):
        while 1:
            data = clientsock.recv(self.BUFFSIZE)
            if not data: break
            request = data.rstrip("\r\n")
            tokens = request.split(" ")
            self.mutex.acquire()
            if tokens[0].lower() == "set":
                reply_str = self.write_to_db(tokens[1:])
            elif tokens[0].lower() == "get":
                reply_str = self.get_from_db(tokens[1:])
            elif tokens[0].lower() == "del":
                reply_str = self.del_from_db(tokens[1:])
            elif tokens[0].lower() == "exit":
                break
            else:
                reply_str = "*** ERROR: UNKNOWN COMMAND"
            #
            self.mutex.release()
            reply_str += "\r\n"
            clientsock.send(reply_str)
        clientsock.close()

#-----------------------------------------------------------------------------------
#    write_to_db
#-----------------------------------------------------------------------------------
    def write_to_db(self, tokens):
        try:
            table = tokens[0]
            item = {}
            item['name'] = tokens[1]
            item['data'] = tokens[2]
            self.db.table(table).remove(where ('name') == item['name'])
            #db.update({'value': 2}, Query)
            self.db.table(table).insert(item)
            if self.notif_function:
                self.notif_function( table , self.db.table(table).all())
        except Exception,e:
            traceback.print_exc()
            return '*** ERROR: '+ str(e)
        return 'OK'

#-----------------------------------------------------------------------------------
#    write_to_db
#-----------------------------------------------------------------------------------
    def get_from_db(self, tokens):
        try:
            table = tokens[0]
            tbl = self.db.table(table)
            resp = str( tbl.all())
        except Exception,e:
            return '*** ERROR: '+ str(e)
        return resp

#-----------------------------------------------------------------------------------
#    del_from_db
#-----------------------------------------------------------------------------------
    def del_from_db(self, tokens):
        try:
            table = tokens[0]
            if len(tokens) > 1:
                name = tokens[1]
                self.db.table(table).remove(where ('name') == name)
            else:
                self.db.purge_table(table)
            if self.notif_function:
                self.notif_function( table , self.db.table(table).all())
        except Exception,e:
            return '*** ERROR: '+ str(e)
        return 'OK'

if __name__ == '__main__':
    srv = infoServerThread(55004)
    srv.start()
    srv.join()
    
