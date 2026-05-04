import time
import argparse
import threading

from luxai.magpie.transport import ZMQRpcRequester
from luxai.magpie.utils import Logger


def worker(client: ZMQRpcRequester, worker_id: int):
    for i in range(3):
        req = {"worker": worker_id, "count": i}
        ret = client.call(req, timeout=5.0)
        Logger.info(f"[worker {worker_id}] got {ret}")


if __name__ == "__main__":
    Logger.set_level("DEBUG")

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "endpoint",
        help="socket endpoint (e.g. tcp://127.0.0.1:5555)"
    )
    args = parser.parse_args()

    client = ZMQRpcRequester(args.endpoint)

    threads = []
    for wid in range(4):  # 4 concurrent callers
        t = threading.Thread(target=worker, args=(client, wid))
        t.start()
        threads.append(t)

    for t in threads:
        t.join()

    Logger.info("All concurrent calls completed")

