"""Qdrant collection management and vector storage."""

from __future__ import annotations

import socket
import uuid

from qdrant_client import QdrantClient
from qdrant_client.models import (
    Distance,
    FieldCondition,
    Filter,
    MatchValue,
    PayloadSchemaType,
    PointStruct,
    VectorParams,
)

from .chunker import Chunk
from .embedder import DIMENSIONS

# Force IPv4 globally — workaround for servers with broken IPv6 (AAAA records).
# httpx/httpcore resolves DNS and tries IPv6 first; if the server doesn't
# actually listen on IPv6, the connection fails.
_original_getaddrinfo = socket.getaddrinfo


def _ipv4_getaddrinfo(host, port, family=0, type=0, proto=0, flags=0):
    if family == 0:
        family = socket.AF_INET
    return _original_getaddrinfo(host, port, family, type, proto, flags)


socket.getaddrinfo = _ipv4_getaddrinfo


def _make_client(url: str, api_key: str | None) -> QdrantClient:
    """Create a Qdrant client.

    QdrantClient ignores the port in the URL and defaults to 6333.
    We parse the URL and pass host/port/https explicitly.
    """
    from urllib.parse import urlparse

    parsed = urlparse(url)
    port = parsed.port or (443 if parsed.scheme == "https" else 6333)

    kwargs: dict = {
        "host": parsed.hostname,
        "port": port,
        "https": parsed.scheme == "https",
        "timeout": 60,
    }
    if api_key:
        kwargs["api_key"] = api_key
    return QdrantClient(**kwargs)


def _deterministic_id(source_url: str, chunk_index: int) -> str:
    """Generate a deterministic UUID from url + chunk index for idempotent upserts."""
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"{source_url}#chunk-{chunk_index}"))


def ensure_collection(url: str, api_key: str | None, collection_name: str) -> None:
    """Create a collection if it doesn't exist, with optimized settings for RAG."""
    client = _make_client(url, api_key)

    existing = [c.name for c in client.get_collections().collections]
    if collection_name in existing:
        return

    client.create_collection(
        collection_name=collection_name,
        vectors_config=VectorParams(
            size=DIMENSIONS,
            distance=Distance.COSINE,
        ),
    )

    # Create payload indexes for efficient filtering
    client.create_payload_index(
        collection_name=collection_name,
        field_name="source_url",
        field_schema=PayloadSchemaType.KEYWORD,
    )
    client.create_payload_index(
        collection_name=collection_name,
        field_name="site_name",
        field_schema=PayloadSchemaType.KEYWORD,
    )


def upsert_chunks(
    url: str,
    api_key: str | None,
    collection_name: str,
    chunks: list[Chunk],
    embeddings: list[list[float]],
) -> int:
    """Upsert chunks with embeddings into a Qdrant collection. Returns count."""
    client = _make_client(url, api_key)

    points = []
    for chunk, embedding in zip(chunks, embeddings):
        point_id = _deterministic_id(
            chunk.metadata["source_url"], chunk.metadata["chunk_index"]
        )
        points.append(
            PointStruct(
                id=point_id,
                vector=embedding,
                payload={**chunk.metadata, "text": chunk.text},
            )
        )

    # Upsert in batches of 100
    batch_size = 100
    for i in range(0, len(points), batch_size):
        batch = points[i : i + batch_size]
        client.upsert(collection_name=collection_name, points=batch)

    return len(points)


def delete_site_from_collection(
    url: str, api_key: str | None, collection_name: str, site_url: str
) -> None:
    """Delete all points from a specific site URL within a collection."""
    client = _make_client(url, api_key)
    client.delete(
        collection_name=collection_name,
        points_selector=Filter(
            must=[FieldCondition(key="source_url", match=MatchValue(value=site_url))]
        ),
    )


def delete_collection(url: str, api_key: str | None, collection_name: str) -> bool:
    """Delete an entire collection. Returns True if it existed."""
    client = _make_client(url, api_key)
    existing = [c.name for c in client.get_collections().collections]
    if collection_name not in existing:
        return False
    client.delete_collection(collection_name)
    return True


def list_collections(url: str, api_key: str | None) -> list[dict]:
    """List all collections with point counts."""
    client = _make_client(url, api_key)
    result = []
    for coll in client.get_collections().collections:
        info = client.get_collection(coll.name)
        result.append(
            {
                "name": coll.name,
                "points": info.points_count,
                "vectors_size": info.config.params.vectors.size
                if hasattr(info.config.params.vectors, "size")
                else "N/A",
            }
        )
    return result
