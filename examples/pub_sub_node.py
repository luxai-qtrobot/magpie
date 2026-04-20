import os, sys
import time

from luxai.magpie.utils import Logger
from luxai.magpie.nodes import BaseNode
from luxai.magpie.transport import ZmqStreamWriter
from luxai.magpie.transport import ZmqStreamReader


class PubNode(BaseNode):
    def __init__(self, endpoint:str):
        self.publisher = ZmqStreamWriter(endpoint)
        super().__init__()

    def process(self):
        Logger.info(f"{self.name} is publishing...")
        self.writer.write({'name': 'Bob', 'last': 'Job'})
        time.sleep(1)

    def cleanup(self):
        self.writer.close()
        Logger.info(f"{self.name} is cleaning up...")
        
    def terminate(self, timeout=None):
        self.writer.close()
        return super().terminate(timeout)


class SubNode(BaseNode):
    def __init__(self, endpoint:str):
        self.subscriber = ZmqStreamReader(endpoint)
        super().__init__()
        
    
    def process(self):        
        data = self.reader.read()
        if data:
            Logger.info(f"{self.name} received {data}")

    def cleanup(self):
        self.reader.close()
        Logger.info(f"{self.name} is cleaning up...")

    def terminate(self, timeout=None):
        self.reader.close()
        return super().terminate(timeout)


if __name__ == '__main__':

    node1 = PubNode(endpoint="tcp://*:5555")
    node2 = SubNode(endpoint="tcp://127.0.0.1:5555")

    try:
        time.sleep(100)
    except KeyboardInterrupt:
        print("Keyboard interupt")
    
    # optionally:        
    # node1.terminate()
    # node2.terminate()
        