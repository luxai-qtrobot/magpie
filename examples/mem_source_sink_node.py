"""
This example demonstrates a simple in-memory communication between two nodes 
using a custom AI framework. The communication is facilitated by a shared memory streamer.

"""

import os, sys
import time

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))

from magpie.utils.logger import Logger
from magpie.nodes.source_node import SourceNode
from magpie.nodes.sink_node import SinkNode
from magpie.transport.local.memory_pushpull import MemmoryPushPull


class ProducerNode(SourceNode):
    def setup(self):
        self.id = 1
    
    def process(self):
        # Logger.info(f"{self.name} is publishing...")
        self.stream_writer.write({self.name: self.id})
        self.id = self.id + 1
        time.sleep(1.0)


class ConsumerNode(SinkNode):

    def process(self):        
        data = self.stream_reader.read()
        if data:
            Logger.info(f"{self.name} received {data}")


if __name__ == '__main__':

    mem_streamer = MemmoryPushPull()
    node1 = ProducerNode(name="node1", stream_writer=mem_streamer)
    node2 = ConsumerNode(name="node2", stream_reader=mem_streamer)
    node3 = ConsumerNode(name="node3", stream_reader=mem_streamer)

    try:
        time.sleep(100)
    except KeyboardInterrupt:
        print("Keyboard interupt")
    finally:        
        node1.terminate()
        node2.terminate()
        node3.terminate()
        