#!/usr/bin/env python3
import argparse
import json
import time
from collections import deque
from time import perf_counter

from luxai.magpie.utils import Logger
from luxai.magpie.nodes import SinkNode
from luxai.magpie.transport import ZMQSubscriber


class ZmqSubscribe(SinkNode):
    """
    Simple Magpie ZMQ topic subscriber tool.

    Prints received messages to stdout.
    Supports:
      --pretty   Pretty JSON output
      --once     Exit after first message
      --hz       Show message frequency (Hz)
    """

    def setup(self, pretty=False, once=False, show_hz=False, hz_window=100):
        self.pretty = pretty
        self.once = once
        self.show_hz = show_hz

        self.msg_count = 0

        # Hz stats
        self.ts_window = deque(maxlen=max(2, hz_window))
        self.last_stats_print = perf_counter()

        Logger.info(
            f"{self.name} subscribing to {self.stream_reader.endpoint}"
        )

    def process(self):
        _data = self.stream_reader.read(timeout=None)
        if _data is None:
            return

        data, _topic = _data  # topic ignored always
        self.msg_count += 1

        # # Print payload
        # if self.pretty:
        #     print(json.dumps(data, indent=2, ensure_ascii=False))
        # else:
        #     print(json.dumps(data, separators=(",", ":"), ensure_ascii=False))

        # Hz calculation
        if self.show_hz:
            now = perf_counter()
            self.ts_window.append(now)

            if (now - self.last_stats_print) >= 1.0:
                hz = self._compute_hz()
                Logger.info(
                    f"rate: {hz:.2f} Hz (msgs={self.msg_count})"
                )
                self.last_stats_print = now
        else:
            # Print payload
            try:
                if self.pretty:
                    print(json.dumps(data, indent=2, ensure_ascii=False))
                else:
                    print(json.dumps(data, separators=(",", ":"), ensure_ascii=False))
            except (TypeError, ValueError):
                print(data)

        # Exit if once
        if self.once:
            Logger.info("Received one message (--once). Exiting...")
            self.terminate()

    def _compute_hz(self) -> float:
        if len(self.ts_window) < 2:
            return 0.0
        dt = self.ts_window[-1] - self.ts_window[0]
        if dt <= 0:
            return 0.0
        return (len(self.ts_window) - 1) / dt


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="magpie-subscribe",
        description="Subscribe to a Magpie ZMQ topic and print received messages",
    )

    p.add_argument(
        "endpoint",
        type=str,
        help="ZMQ endpoint (e.g. tcp://127.0.0.1:5555)",
    )

    p.add_argument(
        "topic",
        type=str,
        help="Topic name (e.g. /motor/joints/state/stream:o)",
    )

    p.add_argument(
        "--pretty",
        action="store_true",
        help="Pretty-print JSON messages",
    )

    p.add_argument(
        "--once",
        action="store_true",
        help="Receive one message and exit",
    )

    p.add_argument(
        "--hz",
        action="store_true",
        help="Show message frequency (Hz)",
    )

    p.add_argument(
        "--hz-window",
        type=int,
        default=100,
        help="Sliding window size for Hz estimation (default: 100)",
    )

    p.add_argument(
        "--bind",
        action="store_true",
        help="Bind the subscriber socket instead of connecting (default: connect).",
    )
    p.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Enable DEBUG logging",
    )

    return p


def main():
    parser = build_parser()
    ns = parser.parse_args()

    Logger.set_level("DEBUG" if ns.verbose else "INFO")

    node = ZmqSubscribe(
        name="MagpieSubscribe",
        stream_reader=ZMQSubscriber(
            endpoint=ns.endpoint,
            topic=ns.topic,
            bind=ns.bind,
        ),
        setup_kwargs={
            "pretty": ns.pretty,
            "once": ns.once,
            "show_hz": ns.hz,
            "hz_window": ns.hz_window,
        },
    )

    # Keep process alive until Ctrl+C or node terminates
    while True:
        try:
            time.sleep(10)
        except KeyboardInterrupt:
            break
    Logger.info("Closing...")
    node.terminate() 
    

if __name__ == "__main__":
    main()
