import argparse
import time

from luxai.magpie.utils import Logger
from luxai.magpie.utils.common import get_uinque_id
from luxai.magpie.discovery import ZconfDiscovery


def advertise_node():
    """
    Advertise a single Magpie node using Zeroconf.
    This runs indefinitely until you press Ctrl+C.
    """
    node_id = get_uinque_id()
    port = 5555
    payload = {"hello": "world"}

    Logger.info(f"Advertising node_id={node_id} on port={port} ...")

    with ZconfDiscovery() as disc:
        disc.advertise_node(
            node_id=node_id,
            port=port,
            payload=payload,
        )

        try:
            while True:
                time.sleep(10)
        except KeyboardInterrupt:
            Logger.info("Advertiser shutting down...")


def scan_nodes():
    """
    Continuously prints the current list of discovered nodes.
    This does not block Zeroconf; it just polls our in-memory view.
    """
    Logger.info("Starting Zeroconf node discovery...")

    disc = ZconfDiscovery()

    try:
        while True:
            time.sleep(2)
            nodes = disc.list_nodes()

            if not nodes:
                Logger.debug("No nodes discovered...")
                continue

            Logger.info("Discovered nodes:")
            for node_id, info in nodes.items():
                best_ip = disc.pick_best_ip(info)
                Logger.info(
                    f"  node_id={node_id}  ips={info.ips}  port={info.port}  payload={info.payload} (best: {best_ip})"
                )

    except KeyboardInterrupt:
        Logger.info("Scanner shutting down...")
    finally:
        disc.close()


if __name__ == "__main__":
    Logger.set_level("DEBUG")

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "role",
        help="Role can be either 'scan' or 'advertise'",
        choices=["scan", "advertise"],
    )

    args = parser.parse_args()

    if args.role == "advertise":
        advertise_node()
    else:
        scan_nodes()
