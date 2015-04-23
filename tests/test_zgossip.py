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
        ctx = zmq.Context()
        # two beacon frames
        self.transmit1 = struct.pack('cccb16sH', b'Z', b'R', b'E',
                           1, uuid.uuid4().bytes,
                           socket.htons(9999))
        self.transmit2 = struct.pack('cccb16sH', b'Z', b'R', b'E',
                           1, uuid.uuid4().bytes,
                           socket.htons(9999))

        self.node1 = ZActor(ctx, ZGossip)
        self.node1.send_unicode("VERBOSE")
        self.node1.send_unicode("BIND", zmq.SNDMORE)
        self.node1.send_unicode("inproc://zgossip")
        
        self.client = zmq.Socket(ctx, zmq.DEALER)
        self.client.setsockopt(zmq.RCVTIMEO, 2000)
        self.client.connect("inproc://zgossip")
        
    # end setUp

    def tearDown(self):
        self.node1.destroy()
    # end tearDown

    def test_ping_pong(self):
        # Send HELLO, which gets no message
        msg = ZGossipMsg(ZGossipMsg.HELLO)
        msg.send(self.client)
        
        # Send PING, expect PONG back
        msg.id = ZGossipMsg.PING
        msg.send(self.client)
        msg.recv(self.client)
        assertTrue(msg.id == ZGossipMsg.PONG)

# end ZGossipTest

if __name__ == '__main__':

    #print(logging.Logger.manager.loggerDict)
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    logger.addHandler(ch)
    logging.getLogger("pyre.zgossip").setLevel(logging.DEBUG)
    logging.getLogger("pyre.zgossip_msg").setLevel(logging.DEBUG)
    
    try:
        unittest.main()
    except Exception as a:
        print(a)
