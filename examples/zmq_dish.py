import os, sys
import time


sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))

from magpie.transport.zmq.zmq_dish import ZMQDish
from magpie.utils.logger import Logger



if __name__ == '__main__':
    dish = ZMQDish("udp://*:5556")

    while True: 
        try:
            data = dish.read()
            Logger.info(f"received  {data}")
            time.sleep(1)
        except KeyboardInterrupt:
            Logger.info('stopping...')   
            dish.close()
            break
    
