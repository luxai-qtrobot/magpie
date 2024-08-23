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

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))

from magpie.transport.stream_writer import StreamWriter
from magpie.utils.logger import Logger
from magpie.nodes.source_node import SourceNode
from magpie.nodes.sink_node import SinkNode
from magpie.transport.zmq.zmq_publisher import ZMQPublisher
from magpie.transport.zmq.zmq_subscriber import ZMQSubscriber


class PubNode(SourceNode):

    def setup(self):
        self.id = 1
        self.data = [1 for _ in range(1_000_000)]

    def process(self):
        # Logger.info(f"{self.name} is publishing...")
        self.stream_writer.write({'count': self.id, 'data': self.data})
        time.sleep(0.2)
        self.id = self.id + 1


if __name__ == '__main__':

    node1 = PubNode(name='node1', stream_writer=ZMQPublisher("tcp://*:5555"))

    try:
        time.sleep(100)
    except KeyboardInterrupt:
        print("Keyboard interupt")
    finally:        
        node1.terminate()
        