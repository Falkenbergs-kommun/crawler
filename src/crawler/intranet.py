"""Crawl Joomla intranet articles via direct database read.

The intranet sits behind SSO, so HTTP scraping needs an authenticated session.
Going straight to the database is both simpler and more reliable: article
bodies live in `<prefix>content.introtext` + `<prefix>content.fulltext` as HTML,
which we clean with trafilatura before handing off to the shared embed+store
flow in external.py.
"""

from __future__ import annotations

import asyncio
from typing import Any

import pymysql
import pymysql.cursors

from .config import AppConfig, IntranetArticleConfig, IntranetDBConfig
from .external import _echo, _embed_and_store_batch
from .store import (
    delete_by_source_urls,
    ensure_collection,
    get_existing_hashes,
)

BATCH_SIZE = 50
STALE_MAX_RATIO = 0.5


def _article_source_url(base_url: str, article_id: int) -> str:
    """Canonical URL we use as Qdrant source_url for a Joomla article."""
    return f"{base_url}/index.php?option=com_content&view=article&id={article_id}"


def _fetch_articles(
    article_ids: list[int], db: IntranetDBConfig
) -> dict[int, dict[str, Any]]:
    """Fetch all requested articles in one query, keyed by id."""
    if not article_ids:
        return {}
    conn = pymysql.connect(
        host=db.host,
        user=db.user,
        password=db.password,
        database=db.name,
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
        connect_timeout=30,
    )
    try:
        placeholders = ",".join(["%s"] * len(article_ids))
        sql = (
            f"SELECT id, title, introtext, `fulltext`, modified, state "
            f"FROM {db.prefix}content WHERE id IN ({placeholders})"
        )
        with conn.cursor() as cur:
            cur.execute(sql, tuple(article_ids))
            rows = cur.fetchall()
    finally:
        conn.close()
    return {int(r["id"]): r for r in rows}


def _extract_text(row: dict[str, Any]) -> str:
    """HTML (introtext + fulltext) → plain text via trafilatura."""
    import trafilatura

    html = (row.get("introtext") or "") + "\n" + (row.get("fulltext") or "")
    if not html.strip():
        return ""
    wrapped = f"<html><body>{html}</body></html>"
    text = trafilatura.extract(
        wrapped,
        include_links=False,
        include_tables=True,
        output_format="txt",
    )
    return (text or "").strip()


async def crawl_intranet_articles(
    articles_config: list[IntranetArticleConfig],
    app_config: AppConfig,
    only_collection: str | None = None,
    only_article_id: int | None = None,
    force: bool = False,
) -> None:
    """Run the intranet pipeline for all configured articles.

    Articles are grouped by collection; each collection becomes its own Qdrant
    collection. Hash-diff and stale-detection follow the same pattern as
    external.py — unchanged articles are skipped, and stale entries (in Qdrant
    but no longer configured) are removed, with a 50 % safety threshold.
    """
    if app_config.intranet_db is None:
        _echo(
            "ERROR: INTRANET_DB_HOST saknas i .env — kan inte köra crawl-intranet."
        )
        return

    # Filter + group
    by_collection: dict[str, list[IntranetArticleConfig]] = {}
    for a in articles_config:
        if only_collection and a.collection != only_collection:
            continue
        if only_article_id is not None and a.article_id != only_article_id:
            continue
        by_collection.setdefault(a.collection, []).append(a)

    if not by_collection:
        _echo("Inga intranet-artiklar matchade filtret.")
        return

    db = app_config.intranet_db

    for coll_name, arts in by_collection.items():
        _echo(f"\n{'=' * 60}")
        _echo(f"Intranet collection: {coll_name}  ({len(arts)} artiklar)")
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

        # One DB query for all articles in this collection
        article_ids = [a.article_id for a in arts]
        _echo(f"  Hämtar {len(article_ids)} artiklar från DB...")
        rows_by_id = await asyncio.to_thread(_fetch_articles, article_ids, db)
        _echo(f"  Fick {len(rows_by_id)} artiklar från DB")

        items: list[dict] = []
        processed_urls: set[str] = set()

        for art in arts:
            row = rows_by_id.get(art.article_id)
            if row is None:
                _echo(f"  [!] Artikel-ID {art.article_id} finns inte i DB — hoppar över")
                continue
            if int(row.get("state", 0)) < 1:
                _echo(
                    f"  [!] Artikel {art.article_id} opublicerad "
                    f"(state={row.get('state')}) — hoppar över"
                )
                continue

            text = _extract_text(row)
            if not text:
                _echo(f"  [!] Artikel {art.article_id} gav tom text — hoppar över")
                continue

            source_url = _article_source_url(db.base_url, art.article_id)
            processed_urls.add(source_url)

            modified = row.get("modified")
            items.append({
                "url": source_url,
                "markdown": text,
                "title": row.get("title") or art.display_name,
                "extra_meta": {
                    "content_type": "intranet_article",
                    "article_id": art.article_id,
                    "modified": modified.isoformat() if modified else None,
                },
            })

        if not items:
            _echo("  Inga publicerbara artiklar.")
            continue

        # Process in batches (each fully embedded+stored before next starts)
        total_stored = 0
        total_skipped = 0
        for start in range(0, len(items), BATCH_SIZE):
            batch = items[start : start + BATCH_SIZE]
            stored, skipped = _embed_and_store_batch(
                batch, existing_hashes, coll_name, app_config, force
            )
            total_stored += stored
            total_skipped += skipped
            batch_num = start // BATCH_SIZE + 1
            _echo(
                f"  Batch {batch_num}: {stored} lagrade, {skipped} oförändrade"
            )

        # Stale detection (same 50% safety threshold as external.py)
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
            f"  Klart: {total_stored} lagrade, {total_skipped} oförändrade,"
            f" {stale_removed} stale borttagna"
        )
