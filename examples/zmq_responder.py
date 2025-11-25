import os, sys
import time




from magpie.transport.zmq.zmq_rpc_responder import ZMQRpcResponder
from magpie.utils.logger import Logger


def on_request(req:object):
    Logger.info(f"on_request: {req}")
    return req

if __name__ == '__main__':
    server = ZMQRpcResponder("tcp://*:5555")

    while True: 
        try:
            status = server.handle_once(handler=on_request, timeout=1.0)
        except TimeoutError:
            Logger.warning(f"zmq_responder example timout on responding...")         
        except KeyboardInterrupt:
            Logger.info('stopping...')   
            server.close()
            break
    
