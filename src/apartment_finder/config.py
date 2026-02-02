"""Configuration loader for the apartment finder."""

import os
from pathlib import Path
from typing import Any, Dict

import yaml
from dotenv import load_dotenv


def load_config(config_path: str = "./config/config.yaml") -> Dict[str, Any]:
    """
    Load configuration from YAML file and environment variables.

    Args:
        config_path: Path to the YAML configuration file

    Returns:
        Dictionary containing merged configuration

    Raises:
        FileNotFoundError: If config file doesn't exist
        ValueError: If config is invalid
    """
    # Load environment variables from .env file
    load_dotenv()

    # Load YAML config
    config_file = Path(config_path)
    if not config_file.exists():
        raise FileNotFoundError(
            f"Configuration file not found: {config_path}\n"
            f"Copy config/config.example.yaml to config/config.yaml and customize it."
        )

    with open(config_file, "r") as f:
        config = yaml.safe_load(f)

    # Validate required sections
    _validate_config(config)

    return config


def _validate_config(config: Dict[str, Any]) -> None:
    """Validate configuration structure."""
    required_sections = ["search", "cities", "email", "scoring"]
    for section in required_sections:
        if section not in config:
            raise ValueError(f"Missing required config section: {section}")

    # Validate search criteria
    search = config["search"]
    if "budget" not in search:
        raise ValueError("Missing budget configuration in search section")
    if "min_usd" not in search["budget"] or "max_usd" not in search["budget"]:
        raise ValueError("Budget must have min_usd and max_usd")

    # Validate scoring weights sum to 1.0
    weights = config.get("scoring", {}).get("weights", {})
    if weights:
        total = sum(weights.values())
        if abs(total - 1.0) > 0.01:
            raise ValueError(f"Scoring weights must sum to 1.0, got {total}")

    # Validate at least one city is configured
    if not config.get("cities"):
        raise ValueError("At least one city must be configured")


def get_env(key: str, default: str = None, required: bool = False) -> str:
    """
    Get environment variable with optional default and required check.

    Args:
        key: Environment variable name
        default: Default value if not set
        required: If True, raise error when not set

    Returns:
        Environment variable value

    Raises:
        ValueError: If required and not set
    """
    value = os.getenv(key, default)
    if required and not value:
        raise ValueError(f"Required environment variable not set: {key}")
    return value
