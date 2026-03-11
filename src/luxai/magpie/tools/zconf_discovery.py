#!/usr/bin/env python3
"""magpie-discovery — scan or advertise Magpie nodes via Zeroconf/mDNS."""

import argparse
import json
import sys
import time

from luxai.magpie.utils.logger import Logger
from luxai.magpie.utils.common import get_uinque_id


def _parse_payload(raw: str):
    try:
        data = json.loads(raw)
    except json.JSONDecodeError as e:
        raise argparse.ArgumentTypeError(f"invalid JSON payload: {e}")
    if not isinstance(data, dict):
        raise argparse.ArgumentTypeError("payload must be a JSON object (dict)")
    return data


def _node_to_dict(info) -> dict:
    return {
        "node_id": info.node_id,
        "ips": info.ips,
        "port": info.port,
        "payload": info.payload,
    }


def _print_nodes(nodes: dict, pretty: bool):
    if not nodes:
        Logger.info("(no nodes found)")
        return
    for node_id, info in sorted(nodes.items()):
        d = _node_to_dict(info)
        if pretty:
            Logger.info(json.dumps(d, indent=2, ensure_ascii=False))
        else:
            Logger.info(json.dumps(d, separators=(",", ":"), ensure_ascii=False))
        print("")

def cmd_scan(args):
    from luxai.magpie.discovery.zconf_discovery import ZconfDiscovery

    Logger.info("magpie-discovery: scanning for nodes — press Ctrl+C to stop")
    Logger.debug(f"magpie-discovery: service_type={args.service_type}, interval={args.interval}s")

    with ZconfDiscovery(service_type=args.service_type) as disc:
        if args.once:
            Logger.debug(f"magpie-discovery: waiting {args.interval}s for responses...")
            time.sleep(args.interval)
            nodes = disc.list_nodes()
            _print_nodes(nodes, args.pretty)
            return

        # Continuous scan
        try:
            while True:
                time.sleep(args.interval)
                nodes = disc.list_nodes()
                _print_nodes(nodes, args.pretty)
        except KeyboardInterrupt:
            pass


def cmd_advertise(args):
    from luxai.magpie.discovery.zconf_discovery import ZconfDiscovery

    node_id = args.id or get_uinque_id()
    payload = args.payload or {}

    Logger.info(f"magpie-discovery: advertising node '{node_id}' on port {args.port} — press Ctrl+C to stop")
    Logger.debug(f"magpie-discovery: service_type={args.service_type}, payload={payload}")

    with ZconfDiscovery(service_type=args.service_type) as disc:
        disc.advertise_node(node_id, port=args.port, payload=payload)
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            pass

    Logger.info("magpie-discovery: stopped.")


def main():
    parser = argparse.ArgumentParser(
        prog="magpie-discovery",
        description="Scan or advertise Magpie nodes via Zeroconf/mDNS.",
    )

    parser.add_argument(
        "--advertise",
        action="store_true",
        help="Switch to advertise mode (default: scan mode).",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=None,
        help="TCP port to advertise (required with --advertise).",
    )
    parser.add_argument(
        "--payload",
        type=_parse_payload,
        default=None,
        metavar="JSON",
        help="Metadata dict for the advertised node, as a JSON object (e.g. '{\"role\":\"robot\"}').",
    )
    parser.add_argument(
        "--id",
        type=str,
        default=None,
        metavar="NODE_ID",
        help="Node ID to advertise. Defaults to an auto-generated ULID.",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=2.0,
        metavar="SECS",
        help="Scan poll interval in seconds (scan mode only, default: 2).",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Scan once and exit (scan mode only).",
    )
    parser.add_argument(
        "--pretty",
        action="store_true",
        help="Pretty-print node info as JSON.",
    )
    parser.add_argument(
        "--service-type",
        type=str,
        default="_magpie-zmq._tcp.local.",
        metavar="TYPE",
        help="Zeroconf service type (default: _magpie-zmq._tcp.local.).",
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Enable DEBUG logging.",
    )

    args = parser.parse_args()

    Logger.set_level("DEBUG" if args.verbose else "INFO")

    if args.advertise:
        if args.port is None:
            parser.error("--port is required with --advertise")
        cmd_advertise(args)
    else:
        cmd_scan(args)


if __name__ == "__main__":
    main()
