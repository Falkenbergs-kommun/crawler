"""Shared lazy-loaded Docling converter instance."""

from __future__ import annotations

# Lazy-initialized docling converters (heavy import, only load when needed)
_converters: dict[bool, object] = {}


def get_converter(ocr: bool = True):
    """Return a shared DocumentConverter instance, created on first call.

    When *ocr* is False, Docling skips all bitmap/OCR processing and only
    extracts programmatically embedded text — much faster for born-digital PDFs.
    """
    if ocr not in _converters:
        from docling.datamodel.pipeline_options import PdfPipelineOptions
        from docling.document_converter import DocumentConverter, PdfFormatOption

        pipeline_options = PdfPipelineOptions(do_ocr=ocr)
        _converters[ocr] = DocumentConverter(
            format_options={"pdf": PdfFormatOption(pipeline_options=pipeline_options)}
        )
    return _converters[ocr]
