
import os, sys
import time


from luxai.magpie.transport import StreamWriter
from luxai.magpie.utils import Logger
from luxai.magpie.nodes import SourceNode
from luxai.magpie.nodes import SinkNode
from luxai.magpie.transport import ZMQPublisher
from luxai.magpie.transport import ZMQSubscriber


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
        