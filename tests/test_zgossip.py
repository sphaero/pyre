import unittest
#import pyre
import zmq
import time
import struct
import uuid
import logging
import socket
from pyre.zactor import ZActor
from pyre.zgossip import ZGossip
from pyre.zgossip_msg import ZGossipMsg

class ZGossipTest(unittest.TestCase):
    
    def setUp(self, *args, **kwargs):
        self.ctx = zmq.Context()
        # two beacon frames
        self.transmit1 = struct.pack('cccb16sH', b'Z', b'R', b'E',
                           1, uuid.uuid4().bytes,
                           socket.htons(9999))
        self.transmit2 = struct.pack('cccb16sH', b'Z', b'R', b'E',
                           1, uuid.uuid4().bytes,
                           socket.htons(9999))

        self.node1 = ZActor(self.ctx, ZGossip)
        self.node1.send_unicode("VERBOSE")
        self.node1.send_unicode("BIND", zmq.SNDMORE)
        self.node1.send_unicode("inproc://zgossip")
    # end setUp

    def tearDown(self):
        self.node1.destroy()
    # end tearDown

    def test_ping_pong(self):
        self.client = zmq.Socket(self.ctx, zmq.DEALER)
        self.client.setsockopt(zmq.RCVTIMEO, 2000)
        self.client.connect("inproc://zgossip")
        # Send HELLO, which gets no message
        msg = ZGossipMsg(ZGossipMsg.HELLO)
        msg.send(self.client)
        
        # Send PING, expect PONG back
        msg.id = ZGossipMsg.PING
        msg.send(self.client)
        msg.recv(self.client)
        self.assertTrue(msg.id == ZGossipMsg.PONG)

    def test_peer_to_peer(self):
        # Set a 100msec timeout on clients so we can test expiry
        #zstr_sendx (base, "SET", "server/timeout", "100", NULL);
        
        self.alpha = ZActor(self.ctx, ZGossip)
        self.alpha.send_unicode("CONNECT", zmq.SNDMORE)
        self.alpha.send_unicode("inproc://zgossip")
        
        self.alpha.send_unicode("PUBLISH", zmq.SNDMORE)
        self.alpha.send_unicode("inproc://alpha-1", zmq.SNDMORE)
        self.alpha.send_unicode("service1")
        
        self.alpha.send_unicode("PUBLISH", zmq.SNDMORE)
        self.alpha.send_unicode("inproc://alpha-2", zmq.SNDMORE)
        self.alpha.send_unicode("service1")
        #print(self.node1.recv(zmq.NOBLOCK))
        #print(self.alpha.recv(zmq.NOBLOCK))
        self.alpha.destroy()
        
# end ZGossipTest

if __name__ == '__main__':

    #print(logging.Logger.manager.loggerDict)
    logger = logging.getLogger()
    logging.basicConfig(level=logging.DEBUG)
    #logger.setLevel(logging.DEBUG)
    #ch = logging.StreamHandler()
    #ch.setLevel(logging.DEBUG)
    #logger.addHandler(ch)
    #logging.getLogger("pyre.zgossip").setLevel(logging.DEBUG)
    #logging.getLogger("pyre.zgossip_msg").setLevel(logging.DEBUG)
    
    try:
        unittest.main()
    except Exception as a:
        print(a)
