#  =========================================================================
#   zgossip_msg - ZeroMQ Gossip Protocol
#  
#  Copyright (c) the Contributors as noted in the AUTHORS file.       
#  This file is part of CZMQ, the high-level C binding for 0MQ:       
#  http://czmq.zeromq.org.                                            
#                                                                      
#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at http://mozilla.org/MPL/2.0/.           
#  =========================================================================

#ifndef ZGOSSIP_MSG_H_INCLUDED
#define ZGOSSIP_MSG_H_INCLUDED

""" These are the zgossip_msg messages:
    HELLO - Client says hello to server
        version             number 1    Version = 1
    PUBLISH - Client or server announces a new tuple
        version             number 1    Version = 1
        key                 string      Tuple key, globally unique
        value               longstr     Tuple value, as printable string
        ttl                 number 4    Time to live, msecs
    PING - Client signals liveness
        version             number 1    Version = 1
    PONG - Server responds to ping; note that pongs are not correlated with pings,
    and may be mixed with other commands, and the client should treat any
    incoming traffic as valid activity.
        version             number 1    Version = 1
    INVALID - Server rejects command as invalid
        version             number 1    Version = 1
"""

import struct
import uuid
import zmq
import logging

STRING_MAX = 255

logger = logging.getLogger(__name__)

class ZGossipMsg(object):
    
    HELLO = 1
    PUBLISH = 2
    PING = 3
    PONG = 4
    INVALID = 5
    
    def __init__(self, id=None, *args, **kwargs):
        
        self.id = id
        self.routing_id = None
        self._needle = 0            # Read/write pointer for serialization
        self._ceiling = 0           # Valid upper limit for read pointer
        self.key = ""               # Tuple key, globally unique
        self.value = None           # Tuple value, as printable string
        self.ttl = 0                # Time to live, msecs
        self.struct_data = b''      # bytes of data

    def recv(self, insocket):
        """
        Receive a zgossip_msg from the socket. 
        Blocks if there is no message waiting.
        """
        frames = insocket.recv_multipart()
        if insocket.type == zmq.ROUTER:
            self.routing_id = frames.pop(0)
        
        self.struct_data = frames.pop(0)
        logger.debug(self.struct_data)
        if not self.struct_data:
            logger.debug("Malformed msg")        
            return
        
        # reset needle
        self._needle = 0
        
        self._ceiling = len(self.struct_data)
        
        signature = self._get_number2()
        if signature != (0xAAA0 | 0): 
            logger.debug("Invalid signature {0}".format(signature))
            return None
        
        # Get message id and parse per message type
        self.id = self._get_number1()
        
        version = self._get_number1()
        if version != 1:
            logger.debug("Invalid version {0}".format(version))
            return

        if self.id == ZGossipMsg.HELLO:
            pass
        elif self.id == ZGossipMsg.PUBLISH:
            self.key = self._get_string()
            self.value = self._get_long_string()
            self.ttl =  self._get_number4()
        
        elif self.id == ZGossipMsg.PING:
            pass
        
        elif self.id == ZGossipMsg.PONG:
            pass
        
        elif self.id == ZGossipMsg.INVALID:
            pass
        
        else:
            logger.debug("bad message ID")

    
    def send(self, outsocket):
        """
        Send the zgossip_msg to the output socket
        """
        if outsocket.socket_type == zmq.ROUTER:
            outsocket.send(self.routing_id, zmq.SNDMORE)
        
        self.struct_data = b''
        self._needle = 0
        
        # add signature
        self._put_number2(0xAAA0 | 0)
        self._put_number1(self.id)
        
        if self.id == ZGossipMsg.HELLO:
            self._put_number1(1)
        elif self.id == ZGossipMsg.PUBLISH:
            self._put_number1(1)
            self._put_string(self.key)
            self._put_long_string(self.value)
            self._put_number4(self.ttl)
        elif self.id == ZGossipMsg.PING:
            self._put_number1(1)
        elif self.id == ZGossipMsg.PONG:
            self._put_number1(1)
        if self.id == ZGossipMsg.INVALID:
            self._put_number1(1)

        outsocket.send(self.struct_data)

    # A lot of the zgossip C's method are not Pythonic so skipping those
    
    # (de)serialisation methods
    def _put_number1(self, nr):
        d = struct.pack('>b', nr)
        self.struct_data += d

    def _put_number2(self, nr):
        d = struct.pack('>H', nr)
        self.struct_data += d

    def _put_number4(self, nr):
        d = struct.pack('>I', nr)
        self.struct_data += d

    def _put_number8(self, nr):
        d = struct.pack('>Q', nr)
        self.struct_data += d    

    def _get_number1(self):
        num = struct.unpack_from('>b', self.struct_data, offset=self._needle)
        self._needle += struct.calcsize('>b')
        return num[0]

    def _get_number2(self):
        num = struct.unpack_from('>H', self.struct_data, offset=self._needle)
        self._needle += struct.calcsize('>H')
        return num[0]

    def _get_number4(self):
        num = struct.unpack_from('>I', self.struct_data, offset=self._needle)
        self._needle += struct.calcsize('>I')
        return num[0]

    def _get_number8(self):
        num = struct.unpack_from('>Q', self.struct_data, offset=self._needle)
        self._needle += struct.calcsize('>Q')
        return num[0]
        
    def _put_string(self, s):
        self._put_number1(len(s))
        d = struct.pack('%is' % len(s), s.encode('UTF-8'))
        self.struct_data += d
        
    def _get_string(self):
        s_len = self._get_number1()
        s = struct.unpack_from(str(s_len) + 's', self.struct_data, offset=self._needle)
        self._needle += struct.calcsize('s' * s_len)
        return s[0].decode('UTF-8')

    def _put_long_string(self, s):
        self._put_number4(len(s))
        d = struct.pack('%is' % len(s), s.encode('UTF-8'))
        self.struct_data += d
        
    def _get_long_string(self):
        s_len = self._get_number4()
        s = struct.unpack_from(str(s_len) + 's', self.struct_data, offset=self._needle)
        self._needle += struct.calcsize('s' * s_len)
        return s[0].decode('UTF-8')
