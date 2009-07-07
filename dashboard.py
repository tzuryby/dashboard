#!/usr/bin/env python
# -*- coding: UTF-8 -*-


__author__ = 'Tzury Bar Yochay <tzury.by@reguluslabs.com>'
__version__ = '0.1'
__license__ = 'GPLv3'

import time, sys, socket, signal,exceptions, cPickle
from socket import *

from threading import Thread

from twisted.internet.protocol import DatagramProtocol
from twisted.internet import reactor

def sendDatagram(host, port, datagram):
    s = socket(AF_INET,SOCK_DGRAM)
    s.connect((host, port))
    s.sendall(datagram)
    s.close()
    
def printargs(fn, *args, **kwargs):
    def wrapper(*args, **kwargs):
        print 'args:', args
        print 'kwargs:', kwargs
        return fn(*args, **kwargs)
        
    return wrapper
    
class SynchStorage(dict):
    delCallback = lambda key: None
    setCallback = lambda key, value: None
    
    def __new__(cls, *args, **kwargs):    
        self = dict.__new__(cls, *args, **kwargs)
        self.__dict__ = self
        return self
        
    def __delitem__(self, key):
        ''' del self item and tell others '''
        try:
            self.delCallback(key)
            print 'sent deleted key %s' % (key)
            super(SynchStorage, self).__delitem__(key)
        except exceptions.KeyError:
            pass
            
    def __setitem__(self, key, value):
        ''' udpate self item and tell others '''
        self.setCallback(key, value)
        print 'sent {%s: %s}' % (key, value)
        super(SynchStorage, self).__setitem__(key, value)
        
    def delitem(self, key):
        '''this method called when getting delete form the network'''
        try:
            print 'got deleted key %s' % (key)
            super(SynchStorage, self).__delitem__(key)
        except exceptions.KeyError:
            pass
            
    def setitem(self, key, value):
        '''this method called when getting update form the network'''
        print 'got {%s: %s}' % (key, value)
        super(SynchStorage, self).__setitem__(key, value)
        
class SyncHashProtocol(DatagramProtocol):
    deleteCallBack = lambda key: None
    updateCallBack = lambda key, value: None
    
    def datagramReceived(self, data, (host, port)):
        self.pareRequest(data)
        
    def pareRequest(self, request):
        p = cPickle.loads(request)
        # dict means update
        if isinstance(p, dict):
            for k, v in p.iteritems():
                self._setItem(k,v)
        # anything else treated as keys and means delete
        else:
            self._delItem(p)
            
    def _delItem(self, key):
        self.deleteCallBack(key)
        
    def _setItem(self, key, value):
        self.updateCallBack(key, value)
            
class SyncHashObserver(object):
    def __init__(self, otherServers):
        self.servers = []
        self.hashTable = SynchStorage(
            delCallback = self.onDelete, 
            setCallback = self.onUpdate)
        
        self.protocol = SyncHashProtocol()
        
        self.protocol.deleteCallBack = self.selfDelete
        self.protocol.updateCallBack = self.selfUpdate
        
        self.servers = otherServers
            
    def sendAll(self, data):
        for host, port in self.servers:
            sendDatagram(host, port, data)
    
    def onUpdate(self, key, value):
        self.sendAll(cPickle.dumps({key: value}))
        
    def onDelete(self, key):
        self.sendAll(cPickle.dumps(key))
        
    def selfDelete(self, key):
        self.hashTable.delitem(key)
        
    def selfUpdate(self, key, value):
        self.hashTable.setitem(key, value)
        

dummyLoop = True

def dataManipulationLoop(hashServer, port):    
    time.sleep(3)
    while dummyLoop:
        key = time.time()
        hashServer.hashTable[key] = port
        if int(key) % 7 == 0:
            del hashServer.hashTable[key]
        time.sleep(1.8)
    
def stop(*args):
    global dummyLoop
    dummyLoop = False
    reactor.stop()
    
if __name__ == '__main__':
    try:
        if len(sys.argv) == 3:
            signal.signal(signal.SIGINT, stop)
            
            selfPort = int(sys.argv[1])
            otherPort = int(sys.argv[2])
            
            otherServers = [('localhost', otherPort),]
            
            hashServer = SyncHashObserver(otherServers)

            t = Thread(target=dataManipulationLoop, args=(hashServer, selfPort))
            t.start()

            reactor.listenUDP(selfPort, hashServer.protocol)
            reactor.run(installSignalHandlers=0)
                 
        else:
            print "Usage: python dhash.py self_port other_port"
            
    except exceptions.KeyboardInterrupt:
        stop()    

'''
to test this module open two terminals 
and type the following lines
terminal a: python dashboard.py 4001 4000
terminal b: python dashboard.py 4000 4001
'''