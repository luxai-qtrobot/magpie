import os, sys
import time
import argparse


from luxai.magpie.nodes import ServerNode
from luxai.magpie.transport import ZMQRpcResponder
from luxai.magpie.utils import Logger



def on_request(req: object):
    Logger.info(f"on_request: {req}")
    time.sleep(1)  # simulate some work
    return {"echo": req}


if __name__ == '__main__':
    Logger.set_level("DEBUG")
    
    parser = argparse.ArgumentParser()
    parser.add_argument("-a", "--address", 
                        help="ZeroMQ server socket endpoint (e.g. tcp://*:5555)",
                        default="tcp://*:5555",
                        type=str)


    args = parser.parse_args()    
    server = ServerNode(name="MyServerNode",
                        responder=ZMQRpcResponder(args.address),
                        handler=on_request)

    try:
        while True:
            time.sleep(10)
    except KeyboardInterrupt:
        Logger.info("Stopping server...")
        # optionally server.terminate()