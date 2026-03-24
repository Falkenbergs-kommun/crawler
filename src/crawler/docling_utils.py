"""Shared lazy-loaded Docling converter instance."""

from __future__ import annotations

# Lazy-initialized docling converter (heavy import, only load when needed)
_converter = None


def get_converter():
    """Return a shared DocumentConverter instance, created on first call."""
    global _converter
    if _converter is None:
        from docling.document_converter import DocumentConverter
        _converter = DocumentConverter()
    return _converter
