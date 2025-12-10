import os, sys
import time

from luxai.magpie.utils import Logger
from luxai.magpie.nodes import BaseNode
from luxai.magpie.transport import ZMQPublisher
from luxai.magpie.transport import ZMQSubscriber


class PubNode(BaseNode):
    def __init__(self, endpoint:str):
        self.publisher = ZMQPublisher(endpoint)
        super().__init__()

    def process(self):
        Logger.info(f"{self.name} is publishing...")
        self.publisher.write({'name': 'Bob', 'last': 'Job'})
        time.sleep(1)

    def cleanup(self):
        self.publisher.close()
        Logger.info(f"{self.name} is cleaning up...")
        
    def terminate(self, timeout=None):
        self.publisher.close()
        return super().terminate(timeout)


class SubNode(BaseNode):
    def __init__(self, endpoint:str):
        self.subscriber = ZMQSubscriber(endpoint)
        super().__init__()
        
    
    def process(self):        
        data = self.subscriber.read()
        if data:
            Logger.info(f"{self.name} received {data}")

    def cleanup(self):
        self.subscriber.close()
        Logger.info(f"{self.name} is cleaning up...")

    def terminate(self, timeout=None):
        self.subscriber.close()
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
        