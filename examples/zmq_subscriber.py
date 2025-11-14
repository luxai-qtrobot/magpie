import os, sys
import time


sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))

from magpie.transport.zmq.zmq_subscriber import ZMQSubscriber
from magpie.utils.logger import Logger



if __name__ == '__main__':
    subscriber = ZMQSubscriber("tcp://127.0.0.1:5555", topic='/mytopic')

    while True: 
        try:
            data = subscriber.read()            
            Logger.info(f"received  {data}")
            time.sleep(1)
        except KeyboardInterrupt:
            Logger.info('stopping...')   
            subscriber.close()
            break
    
