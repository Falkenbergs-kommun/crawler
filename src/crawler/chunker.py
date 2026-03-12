"""Split text into chunks optimized for RAG retrieval."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import date

import tiktoken
from langchain_text_splitters import RecursiveCharacterTextSplitter


def content_hash(text: str) -> str:
    """SHA-256 hash of text content, used for change detection."""
    return hashlib.sha256(text.encode()).hexdigest()


@dataclass
class Chunk:
    text: str
    metadata: dict


# Target ~512 tokens per chunk. With cl100k_base (used by text-embedding-3-large),
# 1 token ≈ 4 chars for English, ~3 chars for Swedish. We use token-based splitting.
CHUNK_SIZE = 512
CHUNK_OVERLAP = 100


def _make_splitter() -> RecursiveCharacterTextSplitter:
    """Create a token-aware recursive text splitter."""
    return RecursiveCharacterTextSplitter.from_tiktoken_encoder(
        encoding_name="cl100k_base",
        chunk_size=CHUNK_SIZE,
        chunk_overlap=CHUNK_OVERLAP,
        separators=["\n\n", "\n", ". ", ", ", " ", ""],
    )


def chunk_page(
    markdown: str,
    source_url: str,
    page_title: str,
    site_name: str,
) -> list[Chunk]:
    """Split a page's markdown into chunks with metadata."""
    if not markdown.strip():
        return []

    splitter = _make_splitter()
    texts = splitter.split_text(markdown)

    today = date.today().isoformat()
    md_hash = content_hash(markdown)
    chunks = []

    for i, text in enumerate(texts):
        chunks.append(
            Chunk(
                text=text,
                metadata={
                    "source_url": source_url,
                    "page_title": page_title,
                    "site_name": site_name,
                    "chunk_index": i,
                    "total_chunks": len(texts),
                    "crawl_date": today,
                    "content_hash": md_hash,
                },
            )
        )

    return chunks
