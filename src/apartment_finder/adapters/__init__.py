from typing import Dict, Type

from .base import BaseAdapter, SearchCriteria

# Import adapters - will be populated as we build them
ADAPTER_REGISTRY: Dict[str, Type[BaseAdapter]] = {}


def register_adapter(name: str):
    """Decorator to register an adapter class."""

    def decorator(cls: Type[BaseAdapter]):
        ADAPTER_REGISTRY[name] = cls
        return cls

    return decorator


def get_adapter(source_name: str, config: dict, city_config: dict) -> BaseAdapter:
    """Factory function to create adapter instances."""
    adapter_class = ADAPTER_REGISTRY.get(source_name)
    if not adapter_class:
        raise ValueError(f"Unknown source: {source_name}. Available: {list(ADAPTER_REGISTRY.keys())}")
    return adapter_class(config, city_config)


def list_available_adapters() -> list:
    """Return list of registered adapter names."""
    return list(ADAPTER_REGISTRY.keys())


# Import adapters to register them
from . import craigslist  # noqa: E402, F401
from . import bayut  # noqa: E402, F401
from . import idealista  # noqa: E402, F401
from . import streeteasy  # noqa: E402, F401
from . import renthop  # noqa: E402, F401
from . import findproperties  # noqa: E402, F401
from . import boligportal  # noqa: E402, F401
from . import lejebolig  # noqa: E402, F401

__all__ = [
    "BaseAdapter",
    "SearchCriteria",
    "ADAPTER_REGISTRY",
    "get_adapter",
    "list_available_adapters",
    "register_adapter",
]
