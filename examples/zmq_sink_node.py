"""
This example demonstrates the use of a custom AI framework for integrating ZeroMQ-based 
publish-subscribe communication between nodes. 

In this example:
- `PubNode` is a source node that publishes data (a dictionary with name and last name) to a ZeroMQ 
  publisher endpoint at regular intervals.
- `SubNode` is a sink node that subscribes to the ZeroMQ publisher endpoint, receives the published 
  data, and logs the received data.

Two nodes are created:
1. `PubNode`: Publishes data to an "inproc" (in-process) ZeroMQ endpoint.
2. `SubNode`: Subscribes to the same "inproc" ZeroMQ endpoint and logs the received data.

The nodes run for a specified time or until interrupted by the user (via a keyboard interrupt). 
Upon termination, both nodes are gracefully terminated to ensure all resources are released.
"""

import os, sys
import time



from magpie.transport.stream_writer import StreamWriter
from magpie.utils.logger import Logger
from magpie.nodes.source_node import SourceNode
from magpie.nodes.sink_node import SinkNode
from magpie.transport.zmq.zmq_publisher import ZMQPublisher
from magpie.transport.zmq.zmq_subscriber import ZMQSubscriber



class SubNode(SinkNode):

    def setup(self, delay):
        self.delay = delay

    def process(self):        
        data = self.stream_reader.read()
        if data:
            Logger.info(f"{self.name} received {data['count']}")
        time.sleep(self.delay)


if __name__ == '__main__':

    node2 = SubNode(name='node2', stream_reader=ZMQSubscriber("tcp://127.0.0.1:5555"), setup_kwargs={'delay': 2})    
    node3 = SubNode(name='node3', stream_reader=ZMQSubscriber("tcp://127.0.0.1:5555"), setup_kwargs={'delay': 0.5})    
    
    try:
        time.sleep(100)
    except KeyboardInterrupt:
        print("Keyboard interupt")
    finally:        
        node2.terminate()
        node3.terminate()
        