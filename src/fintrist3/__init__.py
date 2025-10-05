"""Export public namespace"""
from .__about__ import __version__

# Re-export datareader helpers for convenience
from .datareaders import tiingo as tiingo  # noqa: F401
