"""Decode the `__NUXT_DATA__` payload that Nuxt 3 sites embed in their HTML.

The payload is a devalue-style flat JSON array. Element 0 is the root value.
Inside arrays and objects, every value is an index into the flat array, so
shared and repeated values are stored once. A few special forms exist:

- negative indices: -1 undefined, -2 array hole, -3 NaN, -4 +Infinity,
  -5 -Infinity, -6 negative zero
- a list whose first element is a string is a typed value: ["Date", iso],
  ["Set", i...], ["Map", k, v...], ["RegExp", src, flags], ["BigInt", s],
  ["null", k, v...] (null-prototype object), or a Nuxt reducer such as
  ["Reactive", i], ["Ref", i], ["ShallowReactive", i], ["EmptyRef", i]

Fetched content is data only. This module never evaluates anything.
"""

import json
import math
import re
from typing import Any, Dict, List

NUXT_DATA_RE = re.compile(
    r'<script[^>]*\bid="__NUXT_DATA__"[^>]*>(.*?)</script>', re.S
)

_NEGATIVE = {-1: None, -2: None, -3: math.nan, -4: math.inf, -5: -math.inf, -6: -0.0}
# Nuxt reducers that wrap one value (Vue reactivity, errors, islands)
_WRAPPERS = {"Reactive", "ShallowReactive", "Ref", "ShallowRef", "NuxtError", "Island"}
_EMPTY_REFS = {"EmptyRef", "EmptyShallowRef"}
MAX_DEPTH = 200


class NuxtDataError(ValueError):
    """The page has no `__NUXT_DATA__` payload, or it is malformed."""


def extract_payload(html: str) -> List[Any]:
    """Return the raw flat array from a page's `__NUXT_DATA__` script."""
    match = NUXT_DATA_RE.search(html or "")
    if not match:
        raise NuxtDataError("no __NUXT_DATA__ script on the page")
    try:
        payload = json.loads(match.group(1))
    except json.JSONDecodeError as e:
        raise NuxtDataError(f"__NUXT_DATA__ is not valid JSON: {e}") from e
    if not isinstance(payload, list) or not payload:
        raise NuxtDataError("__NUXT_DATA__ is not a non-empty array")
    return payload


def resolve(payload: List[Any]) -> Any:
    """Rebuild the root value from the flat array."""
    cache: Dict[int, Any] = {}

    def node(index: Any, depth: int) -> Any:
        if not isinstance(index, int) or isinstance(index, bool):
            raise NuxtDataError(f"expected an index, got {index!r}")
        if index < 0:
            return _NEGATIVE.get(index)
        if index >= len(payload):
            raise NuxtDataError(f"index {index} out of range")
        if index in cache:
            return cache[index]
        if depth > MAX_DEPTH:
            raise NuxtDataError("payload nests too deeply")
        value = payload[index]

        if isinstance(value, dict):
            result: Any = {}
            cache[index] = result  # set before recursing: payloads can be cyclic
            for key, child in value.items():
                result[key] = node(child, depth + 1)
            return result

        if isinstance(value, list):
            if value and isinstance(value[0], str):
                result = typed(value, depth)
                cache[index] = result
                return result
            result = []
            cache[index] = result
            result.extend(node(child, depth + 1) for child in value)
            return result

        cache[index] = value  # str, number, bool or null literal
        return value

    def typed(value: List[Any], depth: int) -> Any:
        kind, args = value[0], value[1:]
        if kind in _WRAPPERS:
            return node(args[0], depth + 1) if args else None
        if kind in _EMPTY_REFS:
            raw = node(args[0], depth + 1) if args else "_"
            if raw == "_" or not isinstance(raw, str):
                return None
            try:
                return json.loads(raw)
            except json.JSONDecodeError:
                return None
        if kind in ("Date", "RegExp", "BigInt"):
            return args[0] if args else None  # keep the literal text
        if kind == "Set":
            return [node(child, depth + 1) for child in args]
        if kind in ("Map", "null"):
            pairs = {}
            for i in range(0, len(args) - 1, 2):
                key = node(args[i], depth + 1) if kind == "Map" else args[i]
                pairs[str(key)] = node(args[i + 1], depth + 1)
            return pairs
        # Unknown reducer: best effort, take its single payload
        return node(args[0], depth + 1) if len(args) == 1 else None

    return node(0, 0)


def parse_nuxt_data(html: str) -> Any:
    """Extract and resolve a page's `__NUXT_DATA__` in one step."""
    return resolve(extract_payload(html))
