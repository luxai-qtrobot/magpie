"""
Primitive Frame types for magpie.

These are generic, transport-agnostic containers for simple values
that still carry the standard Frame metadata (gid, id, name, timestamp).

They are NOT robot-specific and can be safely used across different domains.

Included types:
    - BoolFrame   : single boolean value
    - IntFrame    : single integer value
    - FloatFrame  : single float value
    - StringFrame : single string value
    - BytesFrame  : arbitrary binary payload
    - DictFrame   : arbitrary mapping/dict payload
    - ListFrame   : arbitrary list/sequence payload
"""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Sequence

from luxai.magpie.frames.frame import Frame


# ---------------------------------------------------------------------------
# Boolean
# ---------------------------------------------------------------------------

@dataclass
class BoolFrame(Frame):
    """Frame carrying a single boolean value in `value`."""

    value: bool = False

    def __str__(self) -> str:
        state = "True" if self.value else "False"
        return f"{self.name}#{self.gid}:{self.id}({state})"


# ---------------------------------------------------------------------------
# Integer
# ---------------------------------------------------------------------------

@dataclass
class IntFrame(Frame):
    """Frame carrying a single integer value in `value`."""

    value: int = 0

    def __str__(self) -> str:
        return f"{self.name}#{self.gid}:{self.id}({self.value})"


# ---------------------------------------------------------------------------
# Float
# ---------------------------------------------------------------------------

@dataclass
class FloatFrame(Frame):
    """Frame carrying a single float value in `value`."""

    value: float = 0.0

    def __str__(self) -> str:
        # %.6g: compact float formatting
        return f"{self.name}#{self.gid}:{self.id}({self.value:.6g})"


# ---------------------------------------------------------------------------
# String
# ---------------------------------------------------------------------------

@dataclass
class StringFrame(Frame):
    """Frame carrying a single string value in `value`."""

    value: str = ""

    def __str__(self) -> str:
        v = self.value
        # Limit long strings in repr for logging
        if len(v) > 40:
            v = v[:37] + "..."
        return f"{self.name}#{self.gid}:{self.id}({v!r})"


# ---------------------------------------------------------------------------
# Bytes / binary blob
# ---------------------------------------------------------------------------

@dataclass
class BytesFrame(Frame):
    """
    Frame carrying arbitrary binary data in `value`.

    `value` is normalized to `bytes` in __post_init__ to allow
    passing e.g. list[int], bytearray, memoryview, etc.
    """

    value: bytes = b""

    def __post_init__(self):
        # base metadata (gid, id, name, timestamp)
        super().__post_init__()

        # normalize to bytes for robustness
        if isinstance(self.value, (bytearray, memoryview)):
            self.value = bytes(self.value)
        elif isinstance(self.value, Sequence) and not isinstance(self.value, (str, bytes)):
            # e.g. list[int] resulting from some serialization
            self.value = bytes(self.value)

    def __str__(self) -> str:
        return f"{self.name}#{self.gid}:{self.id}(len={len(self.value)})"


# ---------------------------------------------------------------------------
# Dict / mapping payload
# ---------------------------------------------------------------------------

@dataclass
class DictFrame(Frame):
    """
    Frame carrying an arbitrary dictionary payload in `value`.

    Use this when you want structured/unstructured JSON-like data
    but still need Frame metadata (gid, timestamp, name).
    """

    value: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        # base metadata
        super().__post_init__()

        # normalize to a plain dict
        if not isinstance(self.value, dict):
            try:
                self.value = dict(self.value)
            except Exception as exc:  # pragma: no cover (defensive)
                raise TypeError(
                    f"DictFrame.value must be dict-like, got: {type(self.value).__name__}"
                ) from exc

    def __str__(self) -> str:
        preview = repr(self.value)
        if len(preview) > 80:
            preview = preview[:77] + "..."
        return f"{self.name}#{self.gid}:{self.id}({preview})"


# ---------------------------------------------------------------------------
# List / sequence payload
# ---------------------------------------------------------------------------

@dataclass
class ListFrame(Frame):
    """
    Frame carrying an arbitrary list payload in `value`.

    `value` is normalized to a plain list in __post_init__ to allow
    passing any sequence type (tuple, range, numpy array, etc.),
    as long as it is iterable.
    """

    value: List[Any] = field(default_factory=list)

    def __post_init__(self):
        # base metadata
        super().__post_init__()

        if not isinstance(self.value, list):
            # best-effort conversion to a list; this will also consume generators
            try:
                self.value = list(self.value)
            except Exception as exc:  # pragma: no cover (defensive)
                raise TypeError(
                    f"ListFrame.value must be list-like, got: {type(self.value).__name__}"
                ) from exc

    def __str__(self) -> str:
        preview = repr(self.value)
        if len(preview) > 80:
            preview = preview[:77] + "..."
        return f"{self.name}#{self.gid}:{self.id}({preview})"
