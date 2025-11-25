import os, sys
import time




from magpie.transport.zmq.zmq_publisher import ZMQPublisher
from magpie.utils.logger import Logger



if __name__ == '__main__':
    publisher = ZMQPublisher("tcp://*:5555")

    id = 1
    while True: 
        try:
            publisher.write({'name': 'Bob', 'last': 'Job'}, topic='/mytopic')
            Logger.info(f'publishing {id} ...')
            id = id + 1
            time.sleep(1)
        except KeyboardInterrupt:
            Logger.info('stopping...')   
            publisher.close()     
            break
