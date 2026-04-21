"""Fetch individual web pages (no crawling) and index to Qdrant.

Used for sources where we only want a single page indexed — e.g. a law text
on riksdagen.se — rather than a full site crawl. Each `single_pages` entry in
config.yaml is one URL assigned to one Qdrant collection.
"""

from __future__ import annotations

import asyncio
import re

import httpx

from .config import AppConfig, SinglePageConfig
from .external import _echo, _embed_and_store_batch
from .store import (
    delete_by_source_urls,
    ensure_collection,
    get_existing_hashes,
)

USER_AGENT = "FalkenbergKommun-RAG-Bot/1.0"
STALE_MAX_RATIO = 0.5


async def _fetch_one(
    client: httpx.AsyncClient, entry: SinglePageConfig
) -> dict | None:
    """Fetch + extract one page. Returns item dict or None on failure."""
    import trafilatura

    try:
        resp = await client.get(entry.url)
        resp.raise_for_status()
        html = resp.text
    except Exception as e:
        _echo(f"  [!] {entry.url} — hämtning misslyckades: {e}")
        return None

    text = trafilatura.extract(
        html,
        include_links=False,
        include_tables=True,
        output_format="txt",
    )
    if not text or not text.strip():
        _echo(f"  [!] {entry.url} — tom text efter extrahering")
        return None

    title = entry.display_name
    m = re.search(r"<title[^>]*>([^<]+)</title>", html, re.IGNORECASE)
    if m:
        title = m.group(1).strip() or entry.display_name

    return {
        "url": entry.url,
        "markdown": text.strip(),
        "title": title,
        "extra_meta": {
            "content_type": "single_page",
            "display_name": entry.display_name,
        },
    }


async def crawl_single_pages(
    pages_config: list[SinglePageConfig],
    app_config: AppConfig,
    only_collection: str | None = None,
    only_url: str | None = None,
    force: bool = False,
) -> None:
    """Run the single-pages pipeline.

    Pages are grouped by collection; each fetch is independent. We reuse
    `_embed_and_store_batch` so hash-diff behavior matches the rest of the
    crawler. Stale detection with the 50 % safety threshold still applies.
    """
    by_collection: dict[str, list[SinglePageConfig]] = {}
    for p in pages_config:
        if only_collection and p.collection != only_collection:
            continue
        if only_url and p.url != only_url:
            continue
        by_collection.setdefault(p.collection, []).append(p)

    if not by_collection:
        _echo("Inga single_pages matchade filtret.")
        return

    async with httpx.AsyncClient(
        headers={"User-Agent": USER_AGENT},
        follow_redirects=True,
        timeout=30,
    ) as client:
        for coll_name, entries in by_collection.items():
            _echo(f"\n{'=' * 60}")
            _echo(f"Single pages collection: {coll_name}  ({len(entries)} sidor)")
            _echo(f"{'=' * 60}")

            ensure_collection(
                app_config.qdrant_url, app_config.qdrant_api_key, coll_name
            )
            existing_hashes = get_existing_hashes(
                app_config.qdrant_url,
                app_config.qdrant_api_key,
                coll_name,
                coll_name,
            )
            _echo(f"  Befintliga vektorer i Qdrant: {len(existing_hashes)}")

            original_existing_urls = set(existing_hashes.keys())
            processed_urls: set[str] = set()
            items: list[dict] = []

            for entry in entries:
                _echo(f"  Hämtar: {entry.url}")
                item = await _fetch_one(client, entry)
                if item is None:
                    continue
                items.append(item)
                processed_urls.add(entry.url)

            if not items:
                _echo("  Inga sidor att indexera.")
                continue

            stored, skipped = _embed_and_store_batch(
                items, existing_hashes, coll_name, app_config, force
            )
            _echo(f"  {stored} lagrade, {skipped} oförändrade")

            # Stale detection (same 50% safety threshold)
            stale = original_existing_urls - processed_urls
            stale_removed = 0
            if stale and original_existing_urls:
                ratio = len(stale) / len(original_existing_urls)
                if ratio > STALE_MAX_RATIO:
                    _echo(
                        f"  WARNING: {len(stale)} stale URLs = {ratio:.0%} av befintliga"
                        f" (tröskel {STALE_MAX_RATIO:.0%}) — hoppar över borttagning."
                    )
                else:
                    stale_removed = len(stale)
                    _echo(f"  Tar bort {stale_removed} stale URL:er ({ratio:.0%})...")
                    delete_by_source_urls(
                        app_config.qdrant_url,
                        app_config.qdrant_api_key,
                        coll_name,
                        stale,
                    )

            _echo(
                f"  Klart: {stored} lagrade, {skipped} oförändrade,"
                f" {stale_removed} stale borttagna"
            )
