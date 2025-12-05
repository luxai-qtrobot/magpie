import os, sys
import time

from luxai.magpie.transport import ZMQSubscriber
from luxai.magpie.utils import Logger

if __name__ == '__main__':
    Logger.set_level("DEBUG")
    subscriber = ZMQSubscriber("tcp://127.0.0.1:5555", topic=['/mytopic'], bind=False)

    while True: 
        try:
            data, topic = subscriber.read(timeout=None)
            Logger.info(f"received topic {topic} : {data}")
        except TimeoutError as e:
            Logger.debug(e)
        except KeyboardInterrupt:
            Logger.info('stopping...')   
            subscriber.close()
            break
    
