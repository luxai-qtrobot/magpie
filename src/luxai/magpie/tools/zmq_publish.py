#!/usr/bin/env python3
import argparse
import ast
import json
import time
from typing import Any, Dict, Optional

from luxai.magpie.utils.logger import Logger
from luxai.magpie.nodes.source_node import SourceNode
from luxai.magpie.transport.zmq.zmq_publisher import ZMQPublisher
from luxai.magpie.frames import DictFrame


def _parse_payload(raw: str) -> Any:
    """
    Parse payload input. Supports:
      - Any JSON/Python literal: string, number, list, dict
      - Python-literal (PowerShell-friendly): "{'name':'Bob'}" or "'hello'"
      - JSON string: '{"name":"Bob"}' or '"hello"'
      - JSON file: '@payload.json'
      - Plain string fallback (when --raw is used)
    """
    raw = raw.strip()

    if raw.startswith("@"):
        path = raw[1:].strip()
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
        except FileNotFoundError:
            raise argparse.ArgumentTypeError(f"payload file not found: {path}")
        except json.JSONDecodeError as e:
            raise argparse.ArgumentTypeError(f"invalid JSON in payload file {path}: {e}")
    else:
        try:
            data = ast.literal_eval(raw)
        except Exception:
            try:
                data = json.loads(raw)
            except json.JSONDecodeError:
                data = raw  # treat as plain string

    return data


class MagpiePublisher(SourceNode):

    def setup(self, topic: str, data: Any,
              rate: Optional[float] = None,
              count: Optional[int] = None,
              loop: bool = False,
              raw: bool = False):
        self.topic = topic
        self.data = data
        self.rate = rate          # Hz, or None for single-shot
        self.count = count        # max messages, or None
        self.loop = loop          # publish forever
        self.raw = raw            # publish as-is, skip DictFrame wrapping
        self._published = 0
        self._write_time = None

        Logger.info(f"{self.name}: topic={self.topic} rate={self.rate}Hz "
                    f"count={self.count} loop={self.loop} raw={self.raw}")

    def process(self):
        self._write_time = time.time()

        payload = self.data if self.raw else DictFrame(value=self.data).to_dict()

        # Single-shot: publish once then just idle (socket stays alive until Ctrl+C)
        if self.rate is None:
            if self._published == 0:
                self.stream_writer.write(payload, topic=self.topic)
                self._published += 1
                Logger.info(f"{self.name}: published 1 message")
            else:
                time.sleep(0.5)
                self.terminate()
            return

        # Rate mode: publish then sleep to maintain rate
        if self.count is None or self._published < self.count:
            self.stream_writer.write(payload, topic=self.topic)
            self._published += 1

            if self.count is not None and self._published >= self.count:
                Logger.info(f"{self.name}: published {self._published} messages (waiting for Ctrl+C)")
        else:
            # count reached, just idle
            time.sleep(0.5)
            self.terminate()
            return

        # ensure frame rate
        time_diff = time.time() - self._write_time
        frame_period = 1.0 / float(self.rate)
        if time_diff < frame_period:
            time.sleep(frame_period - time_diff)


def main():
    parser = argparse.ArgumentParser(
        prog="magpie-publish",
        description="Publish a message to a Magpie ZMQ topic",
    )

    parser.add_argument(
        "endpoint",
        type=str,
        help="ZMQ endpoint (e.g. tcp://127.0.0.1:5555 or tcp://*:5555 when --bind)",
    )
    parser.add_argument(
        "topic",
        type=str,
        help="Topic name (e.g. /mytopic)",
    )
    parser.add_argument(
        "data",
        type=_parse_payload,
        help="Payload as any JSON/Python literal or @file.json. Must be a dict unless --raw is set.",
    )
    parser.add_argument(
        "--rate",
        type=float,
        default=None,
        help="Publish rate in Hz. If omitted, publishes once and keeps socket alive.",
    )
    parser.add_argument(
        "--count",
        type=int,
        default=None,
        help="Number of messages to publish (requires --rate). If omitted with --loop: publish forever.",
    )
    parser.add_argument(
        "--loop",
        action="store_true",
        help="Publish forever (requires --rate).",
    )
    parser.add_argument(
        "--raw",
        action="store_true",
        help="Publish payload as-is without wrapping in DictFrame. Allows any type, not just dict.",
    )
    parser.add_argument(
        "--bind",
        action="store_true",
        help="Bind the publisher socket (e.g. tcp://*:5555). Default is connect mode.",
    )

    parser.add_argument(
        "--timeout",
        type=float,
        default=None,
        help="timeout in seconds (optional). If omitted, waits forever to connect.",
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Enable DEBUG logging",
    )

    args = parser.parse_args()
    
    Logger.set_level("DEBUG" if args.verbose else "INFO")

    if not args.raw and not isinstance(args.data, dict):
        Logger.error("magpie-publish: payload must be a dict when not using --raw")
        return 2
    if args.rate is not None and args.rate <= 0:
        Logger.error("magpie-publish: --rate must be > 0")
        return 2
    if args.loop and args.rate is None:
        Logger.error("magpie-publish: --loop requires --rate")
        return 2
    if args.count is not None and args.count <= 0:
        Logger.info("magpie-publish: --count <= 0, nothing to do")
        return 0

    publisher = ZMQPublisher(endpoint=args.endpoint, bind=args.bind)
    publisher.wait_connect(timeout=args.timeout)

    node = MagpiePublisher(
        name="MagpiePublisher",
        stream_writer = publisher,
        setup_kwargs={
            "topic": args.topic,
            "data": args.data,
            "rate": args.rate,
            "count": args.count,
            "loop": args.loop,
            "raw": args.raw,
        },
    )

    while not node.terminating():
        try:
            time.sleep(1)
        except KeyboardInterrupt:
            break

    Logger.info("Closing...")
    node.terminate()


if __name__ == "__main__":    
    main()