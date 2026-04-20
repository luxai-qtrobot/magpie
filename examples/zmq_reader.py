import os, sys
import time

from luxai.magpie.transport import ZmqStreamReader
from luxai.magpie.utils import Logger

if __name__ == '__main__':
    Logger.set_level("DEBUG")
    subscriber = ZmqStreamReader("tcp://127.0.0.1:5555", topic=['/mytopic'], bind=False)

    while True: 
        try:
            data, topic = reader.read(timeout=None)
            Logger.info(f"received topic {topic} : {data}")
        except TimeoutError as e:
            Logger.debug(e)
        except KeyboardInterrupt:
            Logger.info('stopping...')   
            # optionally reader.close()
            break
    
