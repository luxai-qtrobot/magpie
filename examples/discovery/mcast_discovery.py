import os, sys
import time
import argparse

from luxai.magpie.utils import Logger 
from luxai.magpie.utils.common import get_uinque_id
from luxai.magpie.discovery import McastDiscovery


def advertise_node(interface = "0.0.0.0"): 
    Logger.info(f"Advertising beacon on {interface}...")
    with McastDiscovery(interface=interface) as d:
        d.advertise({
            "node_id": get_uinque_id(),
            "endpoint": "tcp://*:5555",
        })

        try:
            while True:
                time.sleep(10)        
        except KeyboardInterrupt:
            pass


def scan_node(interface = "0.0.0.0"):
    Logger.info(f"Scanning for beacons on {interface}...")
    d = McastDiscovery(interface=interface)
    try:
        while True:
            Logger.debug("scanning...")
            beacons = d.scan(timeout=2.5)
            for be in beacons:
                Logger.info(f"Found beacon {be}")
            
    except KeyboardInterrupt:
        pass
    finally:
        d.stop_advertising()
        Logger.info("Discovery stopped.")


if __name__ == '__main__':
    Logger.set_level("DEBUG")

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "role",
        help="role can be either 'scan' or 'advertise'",
        choices=["scan", "advertise"],
    )

    parser.add_argument(
        "--interface",
        default="0.0.0.0",
        help="Network interface to bind to (default: 0.0.0.0)"
    )

    args = parser.parse_args()

    if args.role == "advertise":
        advertise_node(args.interface)
    else:
        scan_node(args.interface)
