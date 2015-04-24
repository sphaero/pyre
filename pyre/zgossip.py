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

from .zgossip_msg import ZGossipMsg
import random
import zmq
import logging

logger = logging.getLogger(__name__)

class Client(object):
    # State machine constants
    start_state = 1
    have_tuple_state = 2
    connected_state = 3
    external_state = 4

    NULL_event = 0
    terminate_event = 1
    hello_event = 2
    ok_event = 3
    finished_event = 4
    publish_event = 5
    forward_event = 6
    ping_event = 7
    expired_event = 8
    
    state_name = [
        "(NONE)",
        "start",
        "have tuple",
        "connected",
        "external"
        ]
    event_name = [
        "NONE",
        "terminate",
        "HELLO",
        "ok",
        "finished",
        "PUBLISH",
        "forward",
        "PING",
        "expired"
        ]

    
    def __init__(self, server, routing_id):
        self.client = None
        self.server = server
        self.hashkey = None
        self.routing_id = routing_id
        self.unique_id = None
        self.state = Client.start_state
        self.event = Client.NULL_event # current event
        self.next_event = None
        self.exception = None
        self.wakeup = 0
        self.ticket = None
        self.log_prefix = None
        self._tuples_iter = iter({})
    
    def get_first_tuple(self):
        self.tuples_iter = iter(self.server.tuples.items())
        try:
            key, val = next(self._tuples_iter)
        except StopIteration:
            self.next_event = Client.finished_event
            return
        else:
            self.server.message.key = key
            self.server.message.value = val
            self.next_event = Client.ok_event
    
    def get_next_tuple(self):
        try:
            key, val = next(self._tuples_iter)
        except StopIteration:
            self.next_event = Client.finished_event
            return
        else:
            self.server.message.key = key
            self.server.message.value = val
            self.next_event = Client.ok_event
    
    # TODO make a LUT
    # similar to s_protocol_event
    def _get_event(self, msg):
        if msg.id == ZGossipMsg.HELLO:
            return Client.hello_event;
        elif msg.id == ZGossipMsg.PUBLISH:
            return Client.publish_event;
        elif msg.id == ZGossipMsg.PING:
            return Client.ping_event;
        else:
            # Invalid zgossip_msg_t
            return Client.terminate_event;

    def execute(self, event):
        
        self.next_event = event
        # Cancel wakeup timer, if any was pending
        if (self.wakeup):
            #TODO: zloop_timer_end (self->server->loop, self->wakeup);
            self.wakeup = 0

        while self.next_event > 0:
            self.event = self.next_event
            self.next_event = Client.NULL_event
            self.exception = Client.NULL_event
            if self.server.verbose:
                logger.debug("Client state: {0}, event: {1}".format(Client.state_name[self.state], Client.event_name[self.event]))

            if self.state == Client.start_state:
                if self.event == Client.hello_event:
                    if not self.exception:
                        # get first tuple
                        if self.server.verbose:
                            logger.debug("get first tuple")
                        self.get_first_tuple()
                        # weird extra if in gossip engine
                        self.state = Client.have_tuple_state
                elif self.event == Client.ping_event:
                    if not self.exception:
                        # Send Pong
                        if (self.server.verbose):
                            logger.debug("send PONG")
                        self.server.message.id = ZGossipMsg.PONG
                        self.server.message.routing_id = self.routing_id
                        self.server.message.send(self.server.router)
                elif self.event == Client.expired_event:
                    if not self.exception:
                        # terminate
                        logger.debug("terminate")
                        self.next_event = Client.terminate_event
                else:
                    # Handle unexpected protocol events
                    if self.exception:
                        # Send INVALID
                        logger.debug("send INVALID")
                        self.server.message.id = ZGossipMsg.INVALID
                        self.server.message.routing_id = self.routing_id
                        self.server.message.send(self.server.router)
                    if not self.exception:
                        # terminate
                        logger.debug("terminate by exception")
                        self.next_event = Client.terminate_event
            
            elif self.state == Client.have_tuple_state:
                if self.event == Client.ok_event:
                    if not self.exception:
                        # Send PUBLISH
                        logger.debug("Send PUBLISH")
                        self.server.message.id = ZGossipMsg.PUBLISH
                        self.server.message.routing_id = self.routing_id
                        self.server.message.send(self.server.router)
                        # weird extra if in gossip engine
                        # get next tuple
                        logger.debug("get next tuple")
                        self.get_next_tuple()
                elif self.event == Client.finished_event:
                    if not self.exception: 
                        self.state = Client.connected_state
                else:
                    # Handle unexpected internal events
                    logger.warning("unhandled event {0} in {1}".format(Client.event_name[self.event], Client.state_name[self.state]))
            
            elif self.state == self.connected_state:
                if self.event == Client.publish_event:
                    if not self.exception:
                        # store tuple if new
                        logger.debug("store tuple if new")
                elif self.event == Client.forward_event:
                    if not self.exception:
                        # get tuple to forward
                        logger.debug("get tuple to forward")
                        self.get_tuple_to_forward
                        # weird extra if in gossip engine
                        logger.debug("Send PUBLISH")
                        self.server.message.id = ZGossipMsg.PUBLISH
                        self.server.message.routing_id = self.routing_id
                        self.server.message.send(self.server.router)
                elif self.event == Client.ping_event:
                    if not self.exception:
                        # Send PONG
                        logger.debug("send PONG")
                        self.server.message.id = ZGossipMsg.PONG
                        self.server.message.routing_id = self.routing_id
                        self.server.message.send(self.server.router)
                elif self.event == Client.expired_event:
                    if not self.exception:
                        # terminate
                        logger.debug("terminte by expire")
                        self.next_event = Client.terminate_event
                else:
                    # Handle unexpected protocol events
                    if not self.exception:
                        # Send INVALID
                        logger.debug("send INVALID")
                        self.server.message.id = ZGossipMsg.INVALID
                        self.server.message.routing_id = self.routing_id
                        self.server.message.send(self.server.router)                   
                        # weird extra if in gossip engine
                        logger.debug("terminate")
                        self.next_event = Client.terminate_event

            elif self.state == Client.external_state:
                if self.event == Client.ping_event:
                    if not self.exception:
                        # Send PONG
                        logger.debug("send PONG")
                        self.server.message.id = ZGossipMsg.PONG
                        self.server.message.routing_id = self.routing_id
                        self.server.message.send(self.server.router)
                elif self.event == Client.expired_event:
                    if not self.exception:
                        # terminate
                        logger.debug("terminte by expire")
                        self.next_event = Client.terminate_event
                else:
                    if not self.exception:
                        # Send INVALID
                        logger.debug("send INVALID")
                        self.server.message.id = ZGossipMsg.INVALID
                        self.server.message.routing_id = self.routing_id
                        self.server.message.send(self.server.router)                   
                        # weird extra if in gossip engine
                        logger.debug("terminate")
                        self.next_event = Client.terminate_event
            
            #  If we had an exception event, interrupt normal programming
            if self.exception:
                logger.debug("{0}".format(Client.event_name[self.exception]))
                self.next_event = self.exception
            
            if self.next_event == Client.terminate_event:
                self.server.clients.remove(self.hashkey)
            logger.debug("{0}".format(Client.state_name[self.state]))
    

                
class ZGossip(object):
    
    def __init__(self, ctx, pipe, *args, **kwargs):
        self.ctx = ctx 
        self.pipe = pipe
        self.config = None
        self.port = 0
        self.remotes = []
        self.tuples = {}
        self.cur_tuple = None
        self.terminated = False
        
        # from zproto engine
        self.router = zmq.Socket(self.ctx, zmq.ROUTER)
        # TODO: zsock_set_unbounded (self->router);
        self.clients = {}
        self.client_id = random.randint(0,1000)
        self.timeout = None
        self.verbose = True
        self.log_prefix = ""
        # from server_initialize:
        #TODO: engine_configure (self, "server/timeout", "1000");
        self.message = ZGossipMsg()
        self.run()

    def server_connect(self, endpoint):
        self.remote = zmq.Socket(self.ctx, zmq.DEALER)
        # Never block on sending; we use an infinite HWM and buffer as many
        # messages as needed in outgoing pipes. Note that the maximum number
        # is the overall tuple set size.
        self.remote.set_hwm(0)
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
        #TODO: WHAT IS THIS? zlistx_add_end (self->remotes, remote);
        
    # Process an incoming tuple on this server.
    def server_accept(key, value):
        old_val = self.tuples.get(key)
        if old_val == value:
            return      # Duplicate tuple, do nothing

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
            endpoint = msg.pop(0)
            server_connect(endpoint)
        elif method == "PUBLISH":
            key = msg.pop(0)
            val = msg.pop(0)
            self.server_accept(key, val)
        elif method == "STATUS":
            pass
            # TODO: Return number of tuples we have stored
            #reply = zmsg_new ()
            #assert (reply)
            #zmsg_addstr (reply, "STATUS")
            #zmsg_addstrf (reply, "%d", (int) zhashx_size (self->tuples))
        else:
            logger.debug("unknown zgossip method '%s'"% method)

        return reply

    # Lots of stuff here I don't know what to do with yet
    
    # from zgossip_engine
    def handle_pipe(self):
        #  Get just the commands off the pipe
        request = self.pipe.recv_multipart()
        command = request.pop(0).decode('UTF-8')
        if not command:
            return -1                  #  Interrupted

        if self.verbose:
            logger.debug("API command={0}".format(command))

        if command == "VERBOSE":
            self.verbose = True
        elif command == "$TERM":
            self.terminated = True
        elif command == "BIND":
            # determine if we need to bind_to_random_port("tcp://*")
            # how to get the port number?
            endpoint = request.pop(0).decode('UTF-8')
            self.port = self.router.bind(endpoint)
            logger.debug("Bound to {0} port {1}".format(endpoint, self.port))
            # TODO: handle error??
        elif command == "PORT":
            self.pipe.send_unicode("PORT", zmq.SNDMORE)
            self.pipe.send_unicode(str(self.port))
            port = struct.unpack('I', request.pop(0))[0]
            self.configure(port)
        elif command == "CONFIGURE":
            # TODO: ZConfig class
            filename = request.pop(0).decode('UTF-8')
            self.config.load(filename)
            #self.config = zconfig_load (filename)
            #s_server_config_service (self);
            #self->server.config = self->config
        elif command == "SET":
            # TODO: ZConfig class
            path = request.pop(0).decode('UTF-8')
            value = request.pop(0).decode('UTF-8')
            self.config.put(path, value)
        elif command == "SAVE":
            self.transmit = None
            filename = request.pop(0).decode('UTF-8')
            self.config.save(filename)
        # Custom ZGossip methods
        elif command == "CONNECT":
            endpoint = request.pop(0).decode('UTF-8')
            self.server_connect(endpoint)
        elif command == "PUBLISH":
            key = request.pop(0).decode('UTF-8')
            value = request.pop(0).decode('UTF-8')
            server_accept(key, value)
        elif command == "STATUS":
            #  Return number of tuples we have stored
            self.pipe.send_unicode("STATUS", zmq.SNDMORE)
            self.pipe.send_unicode(str(len(self.tuples)))            
        else:
            logger.error("unkown gossip method: {0}".format(command))

    def handle_protocol(self):
        # We process as many messages as we can, to reduce the overhead
        # of polling and the reactor:
        logger.debug("Handle protocol")
        while self.router.getsockopt(zmq.EVENTS) & zmq.POLLIN:
            self.message.recv(self.router)
            # TODO: use binary hashing on routing_id
            import codecs
            hashkey = codecs.encode(self.message.routing_id, 'hex')
            client = self.clients.get(hashkey)
            if client == None:
                # TODO:
                client = Client(self, self.message.routing_id)
                self.clients[hashkey] = client
            if client.ticket:
                # TODO: Reset a ticket timer, which moves it to the end of the ticket list and
                # resets its execution time. This is a very fast operation.
                #zloop_ticket_reset (zloop_t *self, void *handle);
                #zloop_ticket_reset (self->loop, client->ticket);
                pass
            # Pass to client state machine
            client.execute(client._get_event(self.message))

        logger.debug("end handle protocol")
            

    # Handle messages coming from remotes
    def remote_handler(self, remote):
        msg = self.gossip.recv()
        if msg.id == ZGossipMsg.PUBLISH:
            self.server_accept(msg.key, msg.value)
            
        elif msg.id == ZGossipMsg.INVALID:
            # Connection was reset, so send HELLO again
            msg.id = ZGossipMsg.HELLO
            msg.send(remote)
        
        elif msg.id == ZGossipMsg.PONG:
            pass    # Do nothing with PONGs

    def run(self):
        self.pipe.signal()

        # engine_set_monitor() 
        # Register monitor function that will be called at regular intervals
        # by the server engine
        
        # Set up handler for the two main sockets the server uses
        
        self.poller = zmq.Poller()
        self.poller.register(self.pipe, zmq.POLLIN)
        self.poller.register(self.router, zmq.POLLIN)
        
        while not self.terminated:
            timeout = 1
            #if self.transmit:
            #    timeout = self.ping_at - time.time()
            #    if timeout < 0:
            #        timeout = 0
            # Poll on API pipe and on router socket
            items = dict(self.poller.poll(timeout * 1000))
            if self.pipe in items and items[self.pipe] == zmq.POLLIN:
                self.handle_pipe()
            if self.router in items and items[self.router] == zmq.POLLIN:
                self.handle_protocol()

            #if self.transmit and time.time() >= self.ping_at:
            #    self.send_beacon()
            #    self.ping_at = time.time() + self.interval

            if self.terminated:
                break
        logger.debug("finished")
