
import os, sys
import time

from luxai.magpie.transport import StreamWriter
from luxai.magpie.utils import Logger
from luxai.magpie.nodes import SourceNode
from luxai.magpie.nodes import SinkNode
from luxai.magpie.transport import ZMQPublisher
from luxai.magpie.transport import ZMQSubscriber


class PubNode(SourceNode):

    def setup(self):
        self.id = 1
        self.data = [1 for _ in range(1_000_000)]

    def process(self):
        Logger.info(f"{self.name} is publishing...")
        self.stream_writer.write({'count': self.id, 'data': self.data})
        time.sleep(0.2)
        self.id = self.id + 1


if __name__ == '__main__':
    Logger.set_level("DEBUG")
    node1 = PubNode(name='node1', stream_writer=ZMQPublisher("tcp://*:5555"))

    try:
        time.sleep(100)
    except KeyboardInterrupt:
        print("Keyboard interupt")
    # finally:        
        # node1.terminate()
    Logger.info("exiting...")    