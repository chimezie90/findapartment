"""Logging configuration for the apartment finder."""

import logging
import os
import sys
from datetime import datetime
from pathlib import Path


def setup_logging(log_level: str = None) -> None:
    """
    Configure structured logging for the application.

    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR).
                   Defaults to LOG_LEVEL env var or INFO.
    """
    level = log_level or os.getenv("LOG_LEVEL", "INFO")

    # Create formatter
    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)

    # Optional file handler
    log_dir = Path("./logs")
    file_handler = None
    if log_dir.exists():
        log_file = log_dir / f"apartment_finder_{datetime.now():%Y%m%d}.log"
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(formatter)

    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, level.upper(), logging.INFO))
    root_logger.addHandler(console_handler)
    if file_handler:
        root_logger.addHandler(file_handler)

    # Reduce noise from third-party libraries
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("requests").setLevel(logging.WARNING)
