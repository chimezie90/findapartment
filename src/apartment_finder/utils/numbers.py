"""Number parsing shared by scrapers."""

import re
from typing import Any, Optional

SQM_TO_SQFT = 10.764


def parse_german_number(text: Any) -> Optional[float]:
    """'1.047,25' -> 1047.25; '4,0' -> 4.0; '2.685 €' -> 2685.0. Real numbers
    pass through. Dots are thousands separators, so English decimals like
    '1047.25' are NOT supported -- pass floats instead."""
    if text is None:
        return None
    if isinstance(text, (int, float)):
        return float(text)
    match = re.search(r"\d[\d.]*(?:,\d+)?", str(text))
    if not match:
        return None
    return float(match.group().replace(".", "").replace(",", "."))
