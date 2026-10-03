"""Car listings: model, source adapters, and storage.

Kept apart from the apartment adapters: car sources return Car objects and
take no SearchCriteria (we fetch broadly and filter in the UI).
"""

from typing import Any, Dict, Type

CAR_ADAPTER_REGISTRY: Dict[str, Type] = {}


def register_car_adapter(name: str):
    """Decorator to register a car adapter class."""

    def decorator(cls):
        CAR_ADAPTER_REGISTRY[name] = cls
        return cls

    return decorator


def get_car_adapter(source_name: str, config: Dict[str, Any], city_config: Dict[str, Any]):
    """Factory for car adapter instances."""
    adapter_class = CAR_ADAPTER_REGISTRY.get(source_name)
    if not adapter_class:
        raise ValueError(
            f"Unknown car source: {source_name}. Available: {list(CAR_ADAPTER_REGISTRY)}"
        )
    return adapter_class(config, city_config)


from . import dba, findleasing  # noqa: E402,F401  (registers the adapters)
