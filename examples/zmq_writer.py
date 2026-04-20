import os, sys
import time

from luxai.magpie.transport import ZmqStreamWriter
from luxai.magpie.utils import Logger


if __name__ == '__main__':
    publisher = ZmqStreamWriter("tcp://*:5555", bind=True)

    id = 1
    while True: 
        try:
            writer.write({'name': 'Bob', 'last': 'Job'}, topic='/mytopic')
            Logger.info(f'publishing {id} ...')
            id = id + 1
            time.sleep(1)
        except KeyboardInterrupt:
            Logger.info('stopping...')   
            # optionally writer.close()     
            break
