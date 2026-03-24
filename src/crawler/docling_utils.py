"""Shared lazy-loaded Docling converter instance."""

from __future__ import annotations

# Lazy-initialized docling converters (heavy import, only load when needed)
_converters: dict[bool, object] = {}


def _create_converter(ocr: bool):
    from docling.datamodel.pipeline_options import PdfPipelineOptions
    from docling.document_converter import DocumentConverter, PdfFormatOption

    pipeline_options = PdfPipelineOptions(do_ocr=ocr)
    return DocumentConverter(
        format_options={"pdf": PdfFormatOption(pipeline_options=pipeline_options)}
    )


def get_converter(ocr: bool = True):
    """Return a shared DocumentConverter instance, created on first call.

    When *ocr* is False, Docling skips all bitmap/OCR processing and only
    extracts programmatically embedded text — much faster for born-digital PDFs.
    """
    if ocr not in _converters:
        _converters[ocr] = _create_converter(ocr)
    return _converters[ocr]


def reset_converter(ocr: bool = True):
    """Discard and recreate the converter to free accumulated memory."""
    _converters.pop(ocr, None)
    _converters[ocr] = _create_converter(ocr)
    return _converters[ocr]
