"""Crawl external websites via sitemap discovery and index to Qdrant."""

from __future__ import annotations

import asyncio
import gzip
import re
import tempfile
import xml.etree.ElementTree as ET
from pathlib import Path
from posixpath import splitext
from urllib.parse import urlparse, urlunparse

import click
import httpx

from .chunker import Chunk, chunk_page, content_hash
from .config import AppConfig, ExternalSiteConfig
from .embedder import embed_texts
from .scraper import PageResult
from .store import (
    delete_by_source_urls,
    ensure_collection,
    get_existing_hashes,
    upsert_chunks,
)

_SM_NS = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}

# ---------------------------------------------------------------------------
# 2a. Sitemap parser
# ---------------------------------------------------------------------------


async def parse_sitemaps(
    sitemap_urls: list[str], user_agent: str
) -> dict[str, str | None]:
    """Fetch and parse sitemaps. Returns {url: lastmod_or_None}.

    Handles sitemap indexes, gzipped sitemaps, and plain sitemaps recursively.
    """
    result: dict[str, str | None] = {}

    async with httpx.AsyncClient(
        headers={"User-Agent": user_agent},
        follow_redirects=True,
        timeout=30,
    ) as client:
        await _parse_sitemap_recursive(client, sitemap_urls, result)

    return result


async def _parse_sitemap_recursive(
    client: httpx.AsyncClient,
    urls: list[str],
    result: dict[str, str | None],
) -> None:
    """Recursively fetch and parse sitemaps."""
    for url in urls:
        try:
            resp = await client.get(url)
            resp.raise_for_status()
        except Exception as e:
            click.echo(f"  Warning: failed to fetch sitemap {url}: {e}")
            continue

        # Decompress gzip if needed
        content_type = resp.headers.get("content-type", "")
        data = resp.content
        if (
            url.endswith(".gz")
            or "gzip" in content_type
            or "application/x-gzip" in content_type
        ):
            try:
                data = gzip.decompress(data)
            except Exception as e:
                click.echo(f"  Warning: failed to decompress {url}: {e}")
                continue

        xml_text = data.decode("utf-8", errors="replace")

        try:
            root = ET.fromstring(xml_text)
        except ET.ParseError as e:
            click.echo(f"  Warning: failed to parse XML from {url}: {e}")
            continue

        tag = root.tag.split("}")[-1] if "}" in root.tag else root.tag

        if tag == "sitemapindex":
            # Sitemap index — follow child sitemaps
            child_urls = []
            for sitemap_el in root.findall("sm:sitemap/sm:loc", _SM_NS):
                if sitemap_el.text:
                    child_urls.append(sitemap_el.text.strip())
            click.echo(f"  Sitemap index: {len(child_urls)} child sitemaps")
            await _parse_sitemap_recursive(client, child_urls, result)

        elif tag == "urlset":
            # URL set — extract URLs and lastmod
            count = 0
            for url_el in root.findall("sm:url", _SM_NS):
                loc = url_el.find("sm:loc", _SM_NS)
                lastmod = url_el.find("sm:lastmod", _SM_NS)
                if loc is not None and loc.text:
                    loc_url = loc.text.strip()
                    lm = lastmod.text.strip() if lastmod is not None and lastmod.text else None
                    # Keep most recent lastmod if duplicate
                    if loc_url not in result or (lm and (result[loc_url] is None or lm > result[loc_url])):
                        result[loc_url] = lm
                    count += 1
            click.echo(f"  Parsed {count} URLs from {url.split('/')[-1]}")


# ---------------------------------------------------------------------------
# 2b. URL classification
# ---------------------------------------------------------------------------


def classify_urls(
    urls: dict[str, str | None],
    config: ExternalSiteConfig,
) -> tuple[dict[str, str | None], dict[str, str | None], int]:
    """Split URLs into (pages, documents, skip_count)."""
    pages: dict[str, str | None] = {}
    documents: dict[str, str | None] = {}
    skip_count = 0

    # Pre-compile exclude patterns
    exclude_res = [re.compile(p) for p in config.exclude_patterns]
    doc_exts = set(config.document_extensions)
    skip_exts = set(config.skip_extensions)

    for url, lastmod in urls.items():
        # Check exclude patterns
        if any(r.search(url) for r in exclude_res):
            skip_count += 1
            continue

        # Extract file extension (before query string)
        path = urlparse(url).path
        _, ext = splitext(path)
        ext = ext.lower()

        if ext in skip_exts:
            skip_count += 1
        elif ext in doc_exts:
            documents[url] = lastmod
        else:
            # No extension or unknown extension → treat as web page
            pages[url] = lastmod

    return pages, documents, skip_count


# ---------------------------------------------------------------------------
# URL normalization
# ---------------------------------------------------------------------------


def _normalize_url(url: str) -> str:
    """Normalize URL for deduplication: lowercase host, strip fragment, strip trailing slash."""
    parsed = urlparse(url)
    normalized = urlunparse((
        parsed.scheme.lower(),
        parsed.netloc.lower(),
        parsed.path.rstrip("/") or "/",
        parsed.params,
        parsed.query,
        "",  # strip fragment
    ))
    return normalized


# ---------------------------------------------------------------------------
# 2c. Page fetcher (web pages via httpx + trafilatura)
# ---------------------------------------------------------------------------


async def fetch_pages(
    urls: dict[str, str | None],
    config: ExternalSiteConfig,
) -> list[PageResult]:
    """Fetch web pages with httpx and extract text with trafilatura."""
    import trafilatura

    results: list[PageResult] = []
    sem = asyncio.Semaphore(config.max_concurrent)
    total = len(urls)
    counter = {"done": 0, "failed": 0}

    async def _fetch_one(client: httpx.AsyncClient, url: str) -> None:
        async with sem:
            counter["done"] += 1
            idx = counter["done"]
            try:
                resp = await client.get(url)
                resp.raise_for_status()
                html = resp.text

                text = trafilatura.extract(
                    html,
                    include_links=False,
                    include_tables=True,
                    output_format="txt",
                )

                if text and text.strip():
                    # Extract title from HTML
                    title = ""
                    title_match = re.search(r"<title[^>]*>([^<]+)</title>", html, re.IGNORECASE)
                    if title_match:
                        title = title_match.group(1).strip()

                    results.append(PageResult(
                        url=url,
                        title=title,
                        markdown=text.strip(),
                    ))

                if idx % 100 == 0 or idx == total:
                    click.echo(f"  [page {idx}/{total}] {counter['failed']} failed so far")

            except Exception as e:
                counter["failed"] += 1
                if counter["failed"] <= 10:
                    click.echo(f"  [page {idx}/{total}] Failed: {url} — {e}")
                elif counter["failed"] == 11:
                    click.echo("  (suppressing further page errors)")

            await asyncio.sleep(config.delay_between_requests)

    async with httpx.AsyncClient(
        headers={"User-Agent": config.user_agent},
        follow_redirects=True,
        timeout=30,
    ) as client:
        tasks = [_fetch_one(client, url) for url in urls]
        await asyncio.gather(*tasks)

    click.echo(f"  Pages: {len(results)} OK, {counter['failed']} failed")
    return results


# ---------------------------------------------------------------------------
# 2d. Document fetcher (PDF/DOCX/PPTX via Docling)
# ---------------------------------------------------------------------------


async def fetch_documents(
    urls: dict[str, str | None],
    config: ExternalSiteConfig,
) -> list[PageResult]:
    """Download documents and extract text with Docling."""
    from .docling_utils import get_converter

    results: list[PageResult] = []
    download_sem = asyncio.Semaphore(config.max_concurrent)
    convert_sem = asyncio.Semaphore(2)  # Docling is CPU-heavy
    total = len(urls)
    counter = {"done": 0, "failed": 0}
    max_size = 50 * 1024 * 1024  # 50 MB

    converter = get_converter()

    async def _process_one(client: httpx.AsyncClient, url: str) -> None:
        counter["done"] += 1
        idx = counter["done"]

        # Extract filename and extension
        path = urlparse(url).path
        filename = path.split("/")[-1] or "document"
        _, ext = splitext(path)
        ext = ext.lower() or ".pdf"

        async with download_sem:
            try:
                resp = await client.get(url)
                resp.raise_for_status()
            except Exception as e:
                counter["failed"] += 1
                if counter["failed"] <= 10:
                    click.echo(f"  [doc {idx}/{total}] Download failed: {filename} — {e}")
                return
            finally:
                await asyncio.sleep(config.delay_between_requests)

        data = resp.content
        size_mb = len(data) / (1024 * 1024)

        if len(data) > max_size:
            click.echo(f"  [doc {idx}/{total}] Skipping {filename} ({size_mb:.1f} MB > 50 MB)")
            return

        if idx % 50 == 0 or idx == total:
            click.echo(f"  [doc {idx}/{total}] Processing: {filename} ({size_mb:.1f} MB)")

        # Write to temp file and convert with Docling
        async with convert_sem:
            try:
                text = await asyncio.to_thread(_convert_document, converter, data, ext)
            except Exception as e:
                counter["failed"] += 1
                if counter["failed"] <= 10:
                    click.echo(f"  [doc {idx}/{total}] Convert failed: {filename} — {e}")
                return

        if text and text.strip():
            # Title: first non-empty line from markdown, or filename
            lines = [l.strip().lstrip("#").strip() for l in text.split("\n") if l.strip()]
            title = lines[0][:150] if lines else filename

            doc_format = ext.lstrip(".")
            results.append(PageResult(
                url=url,
                title=title,
                markdown=text.strip(),
                raw_html="",
                external_links=[],
            ))
            # Store document_format in a way the orchestrator can pick up
            results[-1]._doc_format = doc_format  # type: ignore[attr-defined]

    async with httpx.AsyncClient(
        headers={"User-Agent": config.user_agent},
        follow_redirects=True,
        timeout=120,
    ) as client:
        tasks = [_process_one(client, url) for url in urls]
        await asyncio.gather(*tasks)

    click.echo(f"  Documents: {len(results)} OK, {counter['failed']} failed")
    return results


def _convert_document(converter, data: bytes, ext: str) -> str:
    """Write data to tempfile and convert with Docling (runs in thread)."""
    with tempfile.NamedTemporaryFile(suffix=ext, delete=False) as tmp:
        tmp.write(data)
        tmp_path = Path(tmp.name)
    try:
        result = converter.convert(tmp_path)
        return result.document.export_to_markdown()
    finally:
        tmp_path.unlink(missing_ok=True)


# ---------------------------------------------------------------------------
# 2e. Orchestrator
# ---------------------------------------------------------------------------


async def crawl_external_site(
    config: ExternalSiteConfig,
    app_config: AppConfig,
    force: bool = False,
    pages_only: bool = False,
    docs_only: bool = False,
) -> None:
    """Run the full pipeline for an external website."""

    if config.discovery != "sitemap":
        raise NotImplementedError("crawl discovery not yet implemented")

    # 1. Parse sitemaps
    click.echo("Fetching sitemaps...")
    all_urls = await parse_sitemaps(config.sitemaps, config.user_agent)
    click.echo(f"  Total URLs in sitemap: {len(all_urls)}")

    # 2. Classify URLs
    pages, documents, skipped = classify_urls(all_urls, config)
    click.echo(
        f"  {config.name}: {len(pages)} pages, {len(documents)} documents, {skipped} skipped"
    )

    # Apply --pages-only / --docs-only filters
    if pages_only:
        documents = {}
    if docs_only:
        pages = {}

    # 3. Ensure collection exists
    ensure_collection(app_config.qdrant_url, app_config.qdrant_api_key, config.name)

    # 4. Get existing hashes from Qdrant
    existing_hashes = get_existing_hashes(
        app_config.qdrant_url, app_config.qdrant_api_key, config.name, config.name
    )
    click.echo(f"  Existing vectors in Qdrant: {len(existing_hashes)}")

    # 5. Fetch pages
    page_results: list[PageResult] = []
    if pages:
        click.echo(f"\nFetching {len(pages)} web pages...")
        page_results = await fetch_pages(pages, config)

    # 6. Fetch documents
    doc_results: list[PageResult] = []
    if documents:
        click.echo(f"\nProcessing {len(documents)} documents...")
        doc_results = await fetch_documents(documents, config)

    # 7. Build page_content dict and crawled_hashes
    page_content: dict[str, dict] = {}
    crawled_hashes: dict[str, str] = {}

    for page in page_results:
        h = content_hash(page.markdown)
        crawled_hashes[page.url] = h
        page_content[page.url] = {
            "markdown": page.markdown,
            "title": page.title,
            "extra_meta": {
                "content_type": "page",
                "document_format": None,
                "sitemap_lastmod": pages.get(page.url),
            },
        }

    for doc in doc_results:
        h = content_hash(doc.markdown)
        crawled_hashes[doc.url] = h
        doc_format = getattr(doc, "_doc_format", None)
        page_content[doc.url] = {
            "markdown": doc.markdown,
            "title": doc.title,
            "extra_meta": {
                "content_type": "document",
                "document_format": doc_format,
                "sitemap_lastmod": documents.get(doc.url),
            },
        }

    # 8. Content-hash diff against Qdrant
    all_crawled_urls = set(crawled_hashes.keys())
    all_existing_urls = set(existing_hashes.keys())

    if force:
        changed_or_new = all_crawled_urls
        unchanged: set[str] = set()
    else:
        unchanged = {
            url
            for url in all_crawled_urls & all_existing_urls
            if crawled_hashes[url] == existing_hashes[url]
        }
        changed_or_new = all_crawled_urls - unchanged

    # Only mark stale if we crawled the full set (not filtered by pages/docs-only)
    if not pages_only and not docs_only:
        stale = all_existing_urls - all_crawled_urls
    else:
        stale = set()

    click.echo(
        f"\n  {len(all_crawled_urls)} URLs crawled: "
        f"{len(unchanged)} unchanged, {len(changed_or_new)} new/changed, "
        f"{len(stale)} stale"
    )

    # 9. Delete stale URLs
    if stale:
        click.echo(f"  Removing {len(stale)} stale URLs...")
        delete_by_source_urls(
            app_config.qdrant_url, app_config.qdrant_api_key, config.name, stale
        )

    if not changed_or_new:
        click.echo("  Nothing to update.")
        return

    # Delete old vectors for changed URLs before re-upserting
    changed_existing = changed_or_new & all_existing_urls
    if changed_existing:
        click.echo(f"  Removing {len(changed_existing)} changed URLs for re-embedding...")
        delete_by_source_urls(
            app_config.qdrant_url, app_config.qdrant_api_key, config.name, changed_existing
        )

    # 10. Chunk
    click.echo("  Chunking...")
    all_chunks: list[Chunk] = []
    for source_url in sorted(changed_or_new):
        content = page_content[source_url]
        chunks = chunk_page(
            markdown=content["markdown"],
            source_url=source_url,
            page_title=content["title"],
            site_name=config.name,
        )
        for c in chunks:
            c.metadata.update(content["extra_meta"])
        all_chunks.extend(chunks)

    click.echo(f"  {len(all_chunks)} chunks to embed")

    if not all_chunks:
        return

    # 11. Embed
    click.echo("  Generating embeddings...")
    texts = [c.text for c in all_chunks]
    embeddings = embed_texts(texts, app_config.openai_api_key)

    # 12. Store
    click.echo("  Storing in Qdrant...")
    count = upsert_chunks(
        app_config.qdrant_url, app_config.qdrant_api_key, config.name, all_chunks, embeddings
    )
    click.echo(f"  Stored {count} vectors in '{config.name}'")
