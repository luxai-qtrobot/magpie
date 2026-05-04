import os, sys
import time

from luxai.magpie.nodes import BaseNode
from luxai.magpie.utils import Logger


class MyNode(BaseNode):

    def setup(self, message):        
        Logger.info(f"{self.name} is setting up...")
        self.message = message

    def process(self):
        Logger.info(f"{self.name}: {self.message}")
        time.sleep(2)

    def cleanup(self):
        Logger.info(f"{self.name} is cleaning up...")
        
        

if __name__ == '__main__':

    Logger.set_level("DEBUG")
    node = MyNode(name="SimpleNode", setup_kwargs={'message': "Printing"})    
    try:
        time.sleep(5)
        node.pause()
        time.sleep(3)
        node.resume()
        time.sleep(10)        
    except KeyboardInterrupt:
        pass

    # optionally call node.terminate()