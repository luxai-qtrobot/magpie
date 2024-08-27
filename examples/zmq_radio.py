import os, sys
import time


sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))

from magpie.transport.zmq.zmq_radio import ZMQRadio
from magpie.utils.logger import Logger



if __name__ == '__main__':
    radio = ZMQRadio("udp://127.0.0.1:5556")

    id = 1
    while True: 
        try:
            radio.write({'name': 'Ali', 'last': 'paikan'})
            Logger.info(f'publishing {id} ...')
            id = id + 1
            time.sleep(1)
        except KeyboardInterrupt:
            Logger.info('stopping...')   
            radio.close()     
            break
    
