"""Homes for sale: model, source adapters, cost estimates, and storage.

Kept apart from apartments (rentals) and cars: home sources return
HomeListing objects and take no SearchCriteria (we fetch the whole catalog
for the configured area and filter in the UI).
"""

from typing import Any, Dict, Type

HOME_ADAPTER_REGISTRY: Dict[str, Type] = {}


def register_home_adapter(name: str):
    """Decorator to register a home adapter class."""

    def decorator(cls):
        HOME_ADAPTER_REGISTRY[name] = cls
        return cls

    return decorator


def get_home_adapter(source_name: str, config: Dict[str, Any], city_config: Dict[str, Any]):
    """Factory for home adapter instances."""
    adapter_class = HOME_ADAPTER_REGISTRY.get(source_name)
    if not adapter_class:
        raise ValueError(
            f"Unknown home source: {source_name}. Available: {list(HOME_ADAPTER_REGISTRY)}"
        )
    return adapter_class(config, city_config)


from . import homedk  # noqa: E402,F401  (registers the adapter)
