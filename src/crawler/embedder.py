"""Generate embeddings using OpenAI text-embedding-3-large."""

from __future__ import annotations

import time

import click
from openai import OpenAI

MODEL = "text-embedding-3-large"
DIMENSIONS = 3072
BATCH_SIZE = 100
MAX_RETRIES = 3


def embed_texts(texts: list[str], api_key: str) -> list[list[float]]:
    """Embed a list of texts in batches with retry logic."""
    client = OpenAI(api_key=api_key)
    all_embeddings: list[list[float]] = []

    for i in range(0, len(texts), BATCH_SIZE):
        batch = texts[i : i + BATCH_SIZE]
        click.echo(f"  Embedding batch {i // BATCH_SIZE + 1}/{(len(texts) - 1) // BATCH_SIZE + 1} ({len(batch)} texts)")

        for attempt in range(MAX_RETRIES):
            try:
                response = client.embeddings.create(
                    model=MODEL,
                    input=batch,
                    dimensions=DIMENSIONS,
                )
                all_embeddings.extend([e.embedding for e in response.data])
                break
            except Exception as e:
                if attempt < MAX_RETRIES - 1:
                    wait = 2 ** (attempt + 1)
                    click.echo(f"  Retry {attempt + 1}/{MAX_RETRIES} after {wait}s: {e}")
                    time.sleep(wait)
                else:
                    raise

    return all_embeddings
