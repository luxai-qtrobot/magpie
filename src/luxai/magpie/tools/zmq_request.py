import argparse
import json
import sys
import ast
from typing import Any, Dict, Optional

from luxai.magpie.transport import ZMQRpcRequester
from luxai.magpie.utils import Logger


def _parse_args_value(raw: str) -> Dict[str, Any]:
    """
    Parse the args input. Supports:
      - Python-literal dict (PowerShell-friendly): "{'uri':'file:///x.mp3'}"
      - JSON object string: '{"uri":"file:///x.mp3"}'
      - JSON file: '@payload.json'
    """
    import ast

    raw = raw.strip()

    if raw.startswith("@"):
        path = raw[1:].strip()
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
        except FileNotFoundError:
            raise argparse.ArgumentTypeError(f"args file not found: {path}")
        except json.JSONDecodeError as e:
            raise argparse.ArgumentTypeError(f"invalid JSON in args file {path}: {e}")
    else:
        # PowerShell is much happier with "{'k':'v'}" than strict JSON quoting.
        # So: try Python-literal dict first, then JSON.
        try:
            data = ast.literal_eval(raw)
        except Exception:
            try:
                data = json.loads(raw)
            except json.JSONDecodeError as e:
                raise argparse.ArgumentTypeError(
                    f"invalid args. Use JSON like '{{\"uri\":\"file:///x.mp3\"}}' "
                    f"or a Python dict literal like \"{{'uri':'file:///x.mp3'}}\". "
                    f"Error: {e}"
                )

    if not isinstance(data, dict):
        raise argparse.ArgumentTypeError("--args must be a dictionary-like object.")

    return data



def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="magpie-request",
        description="Generic Magpie ZMQ RPC requester",
    )

    p.add_argument(
        "endpoint",
        type=str,
        help="socket endpoint (e.g. tcp://127.0.0.1:5555)",
    )
    p.add_argument(
        "name",
        type=str,
        help="RPC name (e.g. /media/audio/bg/file/play)",
    )
    p.add_argument(
        "args",
        type=_parse_args_value,
        help='RPC args as JSON object string, or @file.json (e.g. \'{"uri":"file:///x.mp3"}\' or @payload.json)',
    )
    p.add_argument(
        "--timeout",
        type=float,
        default=None,
        help="timeout in seconds (optional). If omitted, waits forever.",
    )

    p.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="enable DEBUG logging",
    )

    p.add_argument(
        "--pretty",
        action="store_true",
        help="pretty-print JSON response to stdout",
    )

    return p


def main() -> int:
    parser = build_parser()
    ns = parser.parse_args()

    Logger.set_level("DEBUG" if ns.verbose else "INFO")

    client: Optional[ZMQRpcRequester] = None
    try:
        client = ZMQRpcRequester(ns.endpoint)

        payload = {
            "name": ns.name,
            "args": ns.args,
        }

        ret = client.call(payload, timeout=ns.timeout)

        if ns.pretty:
            print(json.dumps(ret, indent=2, ensure_ascii=False))
        else:
            # print raw (still JSON-serializable in most cases)
            print(ret)

        return 0

    except TimeoutError:
        Logger.warning("magpie-request: timeout on call")
        return 2
    except KeyboardInterrupt:
        Logger.info("magpie-request: interrupted by user")
        return 130
    except Exception as e:
        Logger.error(f"magpie-request: error: {e}")
        return 1
    finally:
        # If your requester exposes close(), call it safely        
        if client is not None and hasattr(client, "close"):
            try:
                client.close()
            except Exception:
                pass


if __name__ == "__main__":
    raise SystemExit(main())
