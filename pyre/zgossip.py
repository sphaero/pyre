# =========================================================================
# zgossip - decentralized configuration management
# Copyright (c) the Contributors as noted in the AUTHORS file.
# This file is part of CZMQ, the high-level C binding for 0MQ:
# http://czmq.zeromq.org.
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
# =========================================================================

# Implements a gossip protocol for decentralized configuration management.
# Your applications nodes form a loosely connected network (which can have
# cycles), and publish name/value tuples. Each node re-distributes the new
# tuples it receives, so that the entire network eventually achieves a
# consistent state. The current design does not expire tuples.
# Provides these commands (sent as multipart strings to the actor):
# * BIND endpoint -- binds the gossip service to specified endpoint
# * PORT -- returns the last TCP port, if any, used for binding
# * LOAD configfile -- load configuration from specified file
# * SET configpath value -- set configuration path = value
# * SAVE configfile -- save configuration to specified file
# * CONNECT endpoint -- connect the gossip service to the specified peer
# * PUBLISH key value -- publish a key/value pair to the gossip cluster
# * STATUS -- return number of key/value pairs held by gossip service
# Returns these messages:
# * PORT number -- reply to PORT command
# * STATUS number -- reply to STATUS command
# * DELIVER key value -- new tuple delivered from network
# @discuss
# The gossip protocol distributes information around a loosely-connected
# network of gossip services. The information consists of name/value pairs
# published by applications at any point in the network. The goal of the
# gossip protocol is to create eventual consistency between all the using
# applications.
# The name/value pairs (tuples) can be used for configuration data, for
# status updates, for presence, or for discovery. When used for discovery,
# the gossip protocol works as an alternative to e.g. UDP beaconing.
# The gossip network consists of a set of loosely-coupled nodes that
# exchange tuples. Nodes can be connected across arbitrary transports,
# so the gossip network can have nodes that communicate over inproc,
# over IPC, and/or over TCP, at the same time.
# Each node runs the same stack, which is a server-client hybrid using
# a modified Harmony pattern (from Chapter 8 of the Guide):
# http://zguide.zeromq.org/page:all# True-Peer-Connectivity-Harmony-Pattern
# Each node provides a ROUTER socket that accepts client connections on an
# key defined by the application via a BIND command. The state machine
# for these connections is in zgossip.xml, and the generated code is in
# zgossip_engine.inc.
# Each node additionally creates outbound connections via DEALER sockets
# to a set of servers ("remotes"), and under control of the calling app,
# which sends CONNECT commands for each configured remote.
# The messages between client and server are defined in zgossip_msg.xml.
# We built this stack using the zeromq/zproto toolkit.
# To join the gossip network, a node connects to one or more peers. Each
# peer acts as a forwarder. This loosely-coupled network can scale to
# thousands of nodes. However the gossip protocol is NOT designed to be
# efficient, and should not be used for application data, as the same
# tuples may be sent many times across the network.
# The basic logic of the gossip service is to accept PUBLISH messages
# from its owning application, and to forward these to every remote, and
# every client it talks to. When a node gets a duplicate tuple, it throws
# it away. When a node gets a new tuple, it stores it, and fowards it as
# just described. At any point the application can access the node's set
# of tuples.
# At present there is no way to expire tuples from the network.
# The assumptions in this design are:
# * The data set is slow-changing. Thus, the cost of the gossip protocol
# is irrelevant with respect to other traffic.

# Writing this while drunk, turn on enhanced issue scanning

class ZGossip(object):
	
	def __init__(self, *args, **kwargs):
		self.pipe
		self.config
		self.remotes = []
		self.tuples = {}
		self.cur_tuple = None
		self.message = None
		
		#engine_configure (self, "server/timeout", "1000");
		#self->message = zgossip_msg_new ();

	def server_connect(self, endpoint):
		self.remote = zmq.socket(zmq.DEALER)
		# Never block on sending; we use an infinite HWM and buffer as many
		# messages as needed in outgoing pipes. Note that the maximum number
		# is the overall tuple set size.
		#TODO:zsock_set_unbounded (remote);
		self.remote.connect(endpoint)
		# Send HELLO and then PUBLISH for each tuple we have
		gossip = ZGossipMsg(ZGossipMsg.HELLO)
		gossip.send(remote)
		for key, value in self.tuples:
			gossip.set_id(ZGossipMsg.PUBLISH)
			gossip.set_key(key)
			gossip.set_value(value)
			gossip.send(remote)
		# Now monitor this remote for incoming messages
		self.handle_socket()
		
	# Process an incoming tuple on this server.
	def server_accept(key, value):
		old_val = self.tuples.get(key)
		if old_val == value:
			return		# Duplicate tuple, do nothing

		# Store new tuple
		self.tuples[key] = value
		
		# Deliver to calling application
		self.pipe.send_unicode("DELIVER", zmq.SNDMORE)
		self.pipe.send_unicode(key)
		self.pipe.send_unicode(value)
		
		# Hold in server context so we can broadcast to all clients
		self.cur_tuple = self.tuples.get(key)
		#TODO: engine_broadcast_event (self, NULL, forward_event);
		
		# Copy new tuple announcement to all remotes
		gossip = ZGossipMsg(ZGossipMsg.PUBLISH)
		for remote in self.remotes:
			gossip.set_key = key
			gossip.set_value = value
			gossip.send(remote)

	# Process server API method, return reply message if any
	def server_method(self, method, msg):
		reply = None
		if method == "CONNECT":
			endpoint = msg.pop()
			server_connect(endpoint)
		elif method == "PUBLISH":
			key = msg.pop()
			val = msg.pop()
			self.server_accept(key, val)
		elif method == "STATUS":
			# Return number of tuples we have stored
			reply = zmsg_new ()
			assert (reply)
			zmsg_addstr (reply, "STATUS")
			zmsg_addstrf (reply, "%d", (int) zhashx_size (self->tuples))
		else:
			logger.debug("unknown zgossip method '%s'"% method)

		return reply
