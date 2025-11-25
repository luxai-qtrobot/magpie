import os, sys
import time
import argparse




from magpie.nodes.server_node import ServerNode
from magpie.transport.zmq.zmq_rpc_responder import ZMQRpcResponder
from magpie.utils.logger import Logger



def on_request(req: object):
    Logger.info(f"on_request: {req}")
    return {"echo": req}


if __name__ == '__main__':
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
        server.terminate()