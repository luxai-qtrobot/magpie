import os, sys
import time
import argparse

from luxai.magpie.transport import ZMQRpcRequester
from luxai.magpie.utils import Logger


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("endpoint", 
                        help="socket endpoint (e.g. tcp://127.0.0.1:5555)",
                        type=str)

    parser.add_argument("id", 
                        help="client name id",
                        type=str)

    args = parser.parse_args()
    client = ZMQRpcRequester(args.endpoint)

    count = 1
    while True: 
        try:            
            ret = client.call({'id': args.id, 'count': count}, timeout=0.5)
            Logger.info(f"client.call got response {ret}")
            count = count + 1
            time.sleep(1)
        except KeyboardInterrupt:
            Logger.info('stopping...')   
            client.close()
            break
