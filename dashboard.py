import time, sys, socket, signal,exceptions
from threading import Thread

import cPickle

from twisted.internet.protocol import DatagramProtocol, ServerFactory, ClientCreator
from twisted.internet import reactor

import socket
from socket import *

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
            super(SynchStorage, self).__delitem__(key)
        except exceptions.KeyError:
            pass
            
    def __setitem__(self, key, value):
        ''' udpate self item and tell others '''
        self.setCallback(key, value)
        super(SynchStorage, self).__setitem__(key, value)
        
    def delitem(self, key):
        '''this method called when getting delete form the network'''
        try:
            super(SynchStorage, self).__delitem__(key)
        except exceptions.KeyError:
            pass
            
    def setitem(self, key, value):
        '''this method called when getting update form the network'''
        super(SynchStorage, self).__setitem__(key, value)
        
class SyncHashProtocol(DatagramProtocol):
    deleteCallBack = None
    updateCallBack = None
    
    def datagramReceived(self, data, (host, port)):
        self.pareRequest(data)
        
    def pareRequest(self, request):
        if request.startswith('delete:'):
            self._delItem(request[7:])
        else:
            p = cPickle.loads(request)
            for k, v in p.iteritems():
                self._setItem(k,v)
            
    def _delItem(self, key):
        if self.deleteCallBack and hasattr(self.deleteCallBack, '__call__'):
            self.deleteCallBack(key)
        
    def _setItem(self, key, value):
        if self.updateCallBack and hasattr(self.updateCallBack, '__call__'):
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
        p = {key: value}
        self.sendAll(cPickle.dumps(p))
        
    def onDelete(self, key):
        self.sendAll('delete:' + key)
        
    def selfDelete(self, key):
        self.hashTable.delitem(key)
        
    def selfUpdate(self, key, value):
        self.hashTable.setitem(key, value)
        print self.hashTable
        
manLoop = True

def dataManipulationLoop(hashServer, port):
    #if port == 4000:
    time.sleep(3)
    while manLoop:
        hashServer.hashTable[time.time()] = port        
        time.sleep(3)
        
    print 'goodbye manipulation'
        
    
def stop(*args):
    global manLoop
    print 'stopping all this noise'
    reactor.stop()
    print 'reactor stopped'
    manLoop = False
    
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
terminal a: python udphash.py 4001 4000
terminal b: python udphash.py 4000 4001
'''