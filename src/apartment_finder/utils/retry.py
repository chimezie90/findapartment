"""Retry decorator with exponential backoff."""

import logging
import time
from functools import wraps
from typing import Tuple, Type

logger = logging.getLogger(__name__)


def retry_with_backoff(
    max_retries: int = 3,
    backoff_factor: float = 2,
    exceptions: Tuple[Type[Exception], ...] = (Exception,),
):
    """
    Decorator for retrying functions with exponential backoff.

    Args:
        max_retries: Maximum number of retry attempts
        backoff_factor: Multiplier for delay between retries (delay = backoff_factor ** attempt)
        exceptions: Tuple of exception types to catch and retry

    Example:
        @retry_with_backoff(max_retries=3, backoff_factor=2)
        def fetch_data():
            ...
    """

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None

            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e
                    if attempt < max_retries:
                        delay = backoff_factor**attempt
                        logger.warning(
                            f"{func.__name__} failed (attempt {attempt + 1}/{max_retries + 1}): {e}. "
                            f"Retrying in {delay}s..."
                        )
                        time.sleep(delay)
                    else:
                        logger.error(f"{func.__name__} failed after {max_retries + 1} attempts: {e}")

            raise last_exception

        return wrapper

    return decorator
