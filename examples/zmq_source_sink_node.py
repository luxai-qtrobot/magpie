
import os, sys
import time

from luxai.magpie.transport import StreamWriter
from luxai.magpie.utils import Logger
from luxai.magpie.nodes import SourceNode
from luxai.magpie.nodes import SinkNode
from luxai.magpie.transport import ZmqStreamWriter
from luxai.magpie.transport import ZmqStreamReader


class PubNode(SourceNode):

    def setup(self):
        self.id = 1
        self.data = [1 for _ in range(10)]

    def process(self):
        # Logger.info(f"{self.name} is publishing...")
        self.stream_writer.write({'count': self.id, 'data': self.data})
        time.sleep(1)
        self.id = self.id + 1


class SubNode(SinkNode):

    def setup(self, delay):
        self.delay = delay

    def process(self):        
        _data = self.stream_reader.read()
        if _data is None:
            return

        data, topic = _data
        if data:
            Logger.info(f"{self.name} received {data['count']}")        


if __name__ == '__main__':

    # node1 = PubNode(name='node1', stream_writer=ZmqStreamWriter("tcp://*:5555"))
    # node2 = SubNode(name='node2', stream_reader=ZmqStreamReader("tcp://127.0.0.1:5555"), setup_kwargs={'delay': 2})    
    # node3 = SubNode(name='node3', stream_reader=ZmqStreamReader("tcp://127.0.0.1:5555"), setup_kwargs={'delay': 0.5})    
    
    node1 = PubNode(name='node1', stream_writer=ZmqStreamWriter("inproc://my_publisher"))
    node2 = SubNode(name='node2', stream_reader=ZmqStreamReader("inproc://my_publisher", queue_size=10), setup_kwargs={'delay': 0})
    node3 = SubNode(name='node3', stream_reader=ZmqStreamReader("inproc://my_publisher", queue_size=10), setup_kwargs={'delay': 2})    

    try:
        time.sleep(100)
    except KeyboardInterrupt:
        print("Keyboard interupt")

    # optionally node1.terminate()
    # optionally node2.terminate()
    # optionally node3.terminate()
        