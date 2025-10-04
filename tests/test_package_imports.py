"""Basic smoke tests for the template package."""
from importlib import import_module


def test_import_package() -> None:
    module = import_module("python_template")
    assert module.__version__ is not None
