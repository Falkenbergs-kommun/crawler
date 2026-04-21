"""Crawl external websites via sitemap discovery and index to Qdrant."""

from __future__ import annotations

import asyncio
import concurrent.futures
import gzip
import re
import tempfile
import time
import xml.etree.ElementTree as ET
from datetime import date
from pathlib import Path
from posixpath import splitext
from urllib.parse import urljoin, urlparse, urlunparse

import click
import httpx


def _echo(msg: str) -> None:
    """click.echo with forced flush for nohup/redirect compatibility."""
    click.echo(msg)
    click.get_text_stream("stdout").flush()

from .chunker import Chunk, chunk_page, content_hash
from .config import AppConfig, ExternalSiteConfig
from .embedder import embed_texts
from .scraper import PageResult
from .store import (
    delete_by_source_urls,
    ensure_collection,
    get_existing_hashes,
    url_exists_in_qdrant,
    upsert_chunks,
)

_SM_NS = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}

# Batch sizes for streaming processing — each batch is fully embedded and
# stored before the next begins, keeping memory bounded and enabling resume.
PAGE_BATCH_SIZE = 200


# ---------------------------------------------------------------------------
# Skip list — persistent record of documents that fail conversion
# ---------------------------------------------------------------------------


def _skip_file_path(config_dir: Path, site_name: str) -> Path:
    """Return path to the skip list TSV for a site (e.g. skip/vardhandboken.tsv)."""
    return config_dir / "skip" / f"{site_name}.tsv"


def _load_skip_list(path: Path) -> dict[str, str]:
    """Load skip list from TSV. Returns {url: error_reason}."""
    if not path.exists():
        return {}
    skips: dict[str, str] = {}
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            parts = line.split("\t")
            if len(parts) >= 3:
                skips[parts[0]] = parts[2]  # url → error
    return skips


def _append_to_skip_list(
    path: Path, url: str, filename: str, error: str
) -> None:
    """Append a failed document to the skip list TSV."""
    path.parent.mkdir(parents=True, exist_ok=True)
    write_header = not path.exists() or path.stat().st_size == 0
    with open(path, "a") as f:
        if write_header:
            f.write("# Documents that failed conversion — skipped on future runs\n")
            f.write("# Remove a line to retry that document on the next crawl\n")
            f.write("# url\tfilename\terror\tdate\n")
        f.write(f"{url}\t{filename}\t{error}\t{date.today().isoformat()}\n")


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
            _echo(f"  Warning: failed to fetch sitemap {url}: {e}")
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
                _echo(f"  Warning: failed to decompress {url}: {e}")
                continue

        xml_text = data.decode("utf-8", errors="replace")

        try:
            root = ET.fromstring(xml_text)
        except ET.ParseError as e:
            _echo(f"  Warning: failed to parse XML from {url}: {e}")
            continue

        tag = root.tag.split("}")[-1] if "}" in root.tag else root.tag

        if tag == "sitemapindex":
            # Sitemap index — follow child sitemaps
            child_urls = []
            for sitemap_el in root.findall("sm:sitemap/sm:loc", _SM_NS):
                if sitemap_el.text:
                    child_urls.append(sitemap_el.text.strip())
            _echo(f"  Sitemap index: {len(child_urls)} child sitemaps")
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
            _echo(f"  Parsed {count} URLs from {url.split('/')[-1]}")


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
# 2c-pre. Linked document discovery
# ---------------------------------------------------------------------------

_HREF_RE = re.compile(r'<a\s[^>]*href=["\']([^"\']+)["\']', re.IGNORECASE)


def _extract_document_links(
    html: str, page_url: str, doc_extensions: set[str]
) -> set[str]:
    """Extract absolute URLs to documents from HTML <a> tags.

    Resolves relative URLs against *page_url* and keeps only those whose
    path ends with one of the *doc_extensions* (e.g. {".pdf", ".docx"}).
    """
    found: set[str] = set()
    for match in _HREF_RE.finditer(html):
        href = match.group(1).strip()
        if not href or href.startswith(("#", "javascript:", "mailto:")):
            continue
        abs_url = urljoin(page_url, href)
        parsed = urlparse(abs_url)
        # Skip non-HTTP URLs (e.g. local file paths like C:/Users/...)
        if parsed.scheme not in ("http", "https"):
            continue
        # Strip query/fragment before checking extension
        _, ext = splitext(parsed.path)
        if ext.lower() in doc_extensions:
            # Normalise: drop fragment, keep query (some CDNs use it)
            clean = urlunparse((
                parsed.scheme, parsed.netloc, parsed.path,
                parsed.params, parsed.query, "",
            ))
            found.add(clean)
    return found


# ---------------------------------------------------------------------------
# 2c. Page fetcher (web pages via httpx + trafilatura)
# ---------------------------------------------------------------------------


async def fetch_pages(
    urls: dict[str, str | None],
    config: ExternalSiteConfig,
    counter_offset: int = 0,
    total_override: int | None = None,
) -> tuple[list[PageResult], set[str]]:
    """Fetch web pages with httpx and extract text with trafilatura.

    Returns (page_results, discovered_document_urls).  When
    ``config.discover_linked_documents`` is True, every fetched page is
    scanned for ``<a href>`` links whose path ends with a document
    extension.  The discovered URLs are returned as the second element.
    """
    import trafilatura

    results: list[PageResult] = []
    discovered_docs: set[str] = set()
    doc_exts = set(config.document_extensions) if config.discover_linked_documents else set()
    sem = asyncio.Semaphore(config.max_concurrent)
    total = total_override or len(urls)
    counter = {"done": 0, "failed": 0}

    async def _fetch_one(client: httpx.AsyncClient, url: str) -> None:
        async with sem:
            counter["done"] += 1
            idx = counter_offset + counter["done"]
            try:
                resp = await client.get(url)
                resp.raise_for_status()
                html = resp.text

                # Discover linked documents before stripping HTML
                if doc_exts:
                    discovered_docs.update(
                        _extract_document_links(html, url, doc_exts)
                    )

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
                    _echo(f"  [page {idx}/{total}] {counter['failed']} failed so far")

            except Exception as e:
                counter["failed"] += 1
                if counter["failed"] <= 10:
                    _echo(f"  [page {idx}/{total}] Failed: {url} — {e}")
                elif counter["failed"] == 11:
                    _echo("  (suppressing further page errors)")

            await asyncio.sleep(config.delay_between_requests)

    async with httpx.AsyncClient(
        headers={"User-Agent": config.user_agent},
        follow_redirects=True,
        timeout=30,
    ) as client:
        tasks = [_fetch_one(client, url) for url in urls]
        await asyncio.gather(*tasks)

    _echo(f"  Pages: {len(results)} OK, {counter['failed']} failed")
    if discovered_docs:
        _echo(f"  Discovered {len(discovered_docs)} linked documents from page content")
    return results, discovered_docs


# ---------------------------------------------------------------------------
# 2d. Document fetcher (PDF/DOCX/PPTX via Docling)
# ---------------------------------------------------------------------------


async def process_documents(
    urls: dict[str, str | None],
    config: ExternalSiteConfig,
    config_name: str,
    app_config: AppConfig,
    existing_hashes: dict[str, str],
    force: bool = False,
    max_new_docs: int | None = None,
) -> tuple[int, int, set[str], int]:
    """Download, convert, embed and store documents sequentially.

    Processes one document at a time to keep memory bounded.  All
    intermediate data (PDF bytes, extracted text, chunks, embeddings)
    is released before moving to the next document.

    Docling conversion runs in a subprocess (ProcessPoolExecutor) so
    that timeouts actually kill the worker process.  Thread-based
    timeouts (asyncio.to_thread) leave zombie threads consuming
    memory, which eventually triggers OOM kills.

    Documents that fail conversion (timeout or exception) are added to
    a persistent TSV skip list (skip/<site>.tsv) so future runs don't
    re-attempt them — timeouts and convert failures are usually stable
    per document (corrupt PDF, unusual layout). Remove a line manually
    to retry a specific document.

    max_new_docs caps the number of documents that reach Docling
    conversion (successes + timeouts + convert failures). Download
    errors and skip-list/Qdrant hits do NOT count against the budget.

    Returns (total_vectors_stored, total_skipped, processed_urls, consumed_budget).
    """
    CONVERT_TIMEOUT = 600  # seconds — skip document if Docling takes longer
    MAX_SIZE = 50 * 1024 * 1024  # 50 MB

    total = len(urls)
    stored_total = 0
    skipped = 0
    failed = 0
    consumed_budget = 0  # docs that reached Docling (stored + timeouts + fails)
    processed_urls: set[str] = set()

    # Load skip list (documents known to fail conversion)
    skip_path = _skip_file_path(app_config.config_dir, config_name)
    skip_list = _load_skip_list(skip_path)
    if skip_list:
        _echo(f"  Skip list loaded: {len(skip_list)} documents ({skip_path.name})")

    # Use a process pool so that Docling timeouts actually kill the worker.
    # asyncio.to_thread uses threads which cannot be killed on timeout —
    # the zombie thread keeps consuming memory until the process is OOM-killed.
    pool = concurrent.futures.ProcessPoolExecutor(max_workers=1)

    async with httpx.AsyncClient(
        headers={"User-Agent": config.user_agent},
        follow_redirects=True,
        timeout=120,
    ) as client:
        for idx, (url, lastmod) in enumerate(urls.items(), 1):
            # --- Budget check (before any work) ---
            if max_new_docs is not None and consumed_budget >= max_new_docs:
                remaining = total - idx + 1
                _echo(
                    f"  Budget reached: max_new_docs={max_new_docs} nya dokument "
                    f"processade. Hoppar över {remaining} återstående dokument."
                )
                break

            # --- Extract filename ---
            path = urlparse(url).path
            filename = path.split("/")[-1] or "document"
            _, ext = splitext(path)
            ext = ext.lower() or ".pdf"

            try:
                # --- Skip list check (doesn't count against budget) ---
                if url in skip_list:
                    skipped += 1
                    processed_urls.add(url)
                    _echo(f"  [doc {idx}/{total}] Skipped (known failure): {filename}")
                    continue

                # --- Live Qdrant check (doesn't count against budget) ---
                if not force and url_exists_in_qdrant(
                    app_config.qdrant_url, app_config.qdrant_api_key, config_name, url
                ):
                    skipped += 1
                    processed_urls.add(url)
                    _echo(f"  [doc {idx}/{total}] Already in Qdrant: {filename}")
                    continue

                # --- Download ---
                resp = None
                try:
                    resp = await client.get(url)
                    resp.raise_for_status()
                    data = resp.content
                except Exception as e:
                    failed += 1
                    if failed <= 10:
                        _echo(f"  [doc {idx}/{total}] Download failed: {filename} — {e!r}")
                    await asyncio.sleep(config.delay_between_requests)
                    continue
                finally:
                    del resp  # release response

                await asyncio.sleep(config.delay_between_requests)

                size_mb = len(data) / (1024 * 1024)
                if len(data) > MAX_SIZE:
                    _echo(f"  [doc {idx}/{total}] Skipping {filename} ({size_mb:.1f} MB > 50 MB)")
                    data = None
                    continue

                _echo(f"  [doc {idx}/{total}] Converting: {filename} ({size_mb:.1f} MB)")

                # --- Convert with Docling (in subprocess with real timeout) ---
                t0 = time.monotonic()
                loop = asyncio.get_running_loop()
                future = loop.run_in_executor(
                    pool, _convert_document_standalone, data, ext, config.ocr
                )
                try:
                    text = await asyncio.wait_for(future, timeout=CONVERT_TIMEOUT)
                except asyncio.TimeoutError:
                    failed += 1
                    consumed_budget += 1
                    err_msg = f"Timeout after {CONVERT_TIMEOUT}s"
                    _echo(f"  [doc {idx}/{total}] {err_msg}: {filename}")
                    _append_to_skip_list(skip_path, url, filename, err_msg)
                    # Kill the stuck worker and create a fresh pool
                    pool.shutdown(wait=False, cancel_futures=True)
                    pool = concurrent.futures.ProcessPoolExecutor(max_workers=1)
                    data = None
                    continue
                except concurrent.futures.process.BrokenProcessPool as e:
                    failed += 1
                    consumed_budget += 1
                    err_msg = f"Worker crashed (BrokenProcessPool): {e!r}"[:200]
                    _echo(
                        f"  [doc {idx}/{total}] Worker crashed: {filename} — recreating pool"
                    )
                    _append_to_skip_list(skip_path, url, filename, err_msg)
                    pool.shutdown(wait=False, cancel_futures=True)
                    pool = concurrent.futures.ProcessPoolExecutor(max_workers=1)
                    data = None
                    continue
                except Exception as e:
                    failed += 1
                    consumed_budget += 1
                    err_msg = repr(e).split("\n")[0][:200]
                    if failed <= 10:
                        _echo(f"  [doc {idx}/{total}] Convert failed: {filename} — {err_msg}")
                    _append_to_skip_list(skip_path, url, filename, err_msg)
                    data = None
                    continue
                finally:
                    data = None  # release PDF bytes

                elapsed = time.monotonic() - t0
                consumed_budget += 1

                if not text or not text.strip():
                    _echo(f"  [doc {idx}/{total}] Empty: {filename} — {elapsed:.1f}s")
                    text = None
                    continue

                text = text.strip()
                processed_urls.add(url)

                # --- Hash check ---
                h = content_hash(text)
                if not force and url in existing_hashes and existing_hashes[url] == h:
                    skipped += 1
                    _echo(f"  [doc {idx}/{total}] Unchanged: {filename}, {elapsed:.1f}s")
                    text = None
                    continue

                # --- Chunk, embed, store ---
                lines = [l.strip().lstrip("#").strip() for l in text.split("\n") if l.strip()]
                title = lines[0][:150] if lines else filename
                doc_format = ext.lstrip(".")

                if url in existing_hashes:
                    delete_by_source_urls(
                        app_config.qdrant_url, app_config.qdrant_api_key, config_name, {url}
                    )

                chunks = chunk_page(
                    markdown=text,
                    source_url=url,
                    page_title=title,
                    site_name=config_name,
                )
                text = None  # release extracted text

                for c in chunks:
                    c.metadata.update({
                        "content_type": "document",
                        "document_format": doc_format,
                        "sitemap_lastmod": lastmod,
                    })

                chunk_texts = [c.text for c in chunks]
                embeddings = await asyncio.to_thread(
                    embed_texts, chunk_texts, app_config.openai_api_key
                )
                chunk_texts = None  # release text list

                count = upsert_chunks(
                    app_config.qdrant_url, app_config.qdrant_api_key,
                    config_name, chunks, embeddings,
                )
                stored_total += count
                existing_hashes[url] = h

                _echo(
                    f"  [doc {idx}/{total}] Stored: {filename} — "
                    f"{len(chunks)} chunks, {elapsed:.1f}s"
                )
                chunks = None
                embeddings = None

            except Exception as e:
                failed += 1
                _echo(f"  [doc {idx}/{total}] Unexpected error: {filename} — {e!r}")
                continue

    pool.shutdown(wait=False)

    cap_str = f"/{max_new_docs}" if max_new_docs is not None else ""
    _echo(
        f"  Documents done: {stored_total} vectors stored, "
        f"{skipped} unchanged, {failed} failed, "
        f"budget consumed: {consumed_budget}{cap_str}"
    )
    return stored_total, skipped, processed_urls, consumed_budget


def _convert_document_standalone(data: bytes, ext: str, ocr: bool) -> str:
    """Write data to tempfile and convert with Docling (runs in subprocess).

    Creates its own converter instance since this runs in a separate process.
    The process is disposable — killed on timeout, no leaked memory.
    """
    from .docling_utils import get_converter

    converter = get_converter(ocr=ocr)
    with tempfile.NamedTemporaryFile(suffix=ext, delete=False) as tmp:
        tmp.write(data)
        tmp_path = Path(tmp.name)
    try:
        result = converter.convert(tmp_path)
        return result.document.export_to_markdown()
    finally:
        tmp_path.unlink(missing_ok=True)


# ---------------------------------------------------------------------------
# 2e. Batch embed + store helper
# ---------------------------------------------------------------------------


def _embed_and_store_batch(
    items: list[dict],
    existing_hashes: dict[str, str],
    config_name: str,
    app_config: AppConfig,
    force: bool,
) -> tuple[int, int]:
    """Hash-check, chunk, embed, and store a batch of content items.

    Each item dict has keys: url, markdown, title, extra_meta.
    Returns (vectors_stored, items_skipped_unchanged).
    Mutates *existing_hashes* in-place to record newly stored hashes.
    """
    new_or_changed: list[dict] = []
    skipped = 0

    for item in items:
        h = content_hash(item["markdown"])
        if not force and item["url"] in existing_hashes and existing_hashes[item["url"]] == h:
            skipped += 1
            continue
        item["_hash"] = h
        new_or_changed.append(item)

    if not new_or_changed:
        return 0, skipped

    # Delete old vectors for changed URLs before re-upserting
    changed_urls = {it["url"] for it in new_or_changed if it["url"] in existing_hashes}
    if changed_urls:
        delete_by_source_urls(
            app_config.qdrant_url, app_config.qdrant_api_key, config_name, changed_urls
        )

    # Chunk
    all_chunks: list[Chunk] = []
    for item in new_or_changed:
        chunks = chunk_page(
            markdown=item["markdown"],
            source_url=item["url"],
            page_title=item["title"],
            site_name=config_name,
        )
        for c in chunks:
            c.metadata.update(item["extra_meta"])
        all_chunks.extend(chunks)

    if not all_chunks:
        return 0, skipped

    # Embed
    texts = [c.text for c in all_chunks]
    embeddings = embed_texts(texts, app_config.openai_api_key)

    # Store
    count = upsert_chunks(
        app_config.qdrant_url, app_config.qdrant_api_key, config_name, all_chunks, embeddings
    )

    # Update existing_hashes so subsequent batches detect already-stored content
    for item in new_or_changed:
        existing_hashes[item["url"]] = item["_hash"]

    return count, skipped


# ---------------------------------------------------------------------------
# 2f. Orchestrator
# ---------------------------------------------------------------------------


async def crawl_external_site(
    config: ExternalSiteConfig,
    app_config: AppConfig,
    force: bool = False,
    pages_only: bool = False,
    docs_only: bool = False,
    max_new_docs: int | None = None,
) -> int:
    """Run the full pipeline for an external website.

    Processes content in streaming batches to limit memory usage. Each batch
    is embedded and stored before the next begins, so interrupted runs can
    be resumed efficiently — already-stored vectors are detected via
    content-hash comparison and skipped.

    max_new_docs caps how many documents reach Docling conversion. When a
    cap is set, stale detection is skipped (an intentionally truncated run
    cannot reliably distinguish "processed" from "not yet reached").

    Returns the number of documents that consumed the budget (for
    cross-site budget tracking in the CLI).
    """

    if config.discovery != "sitemap":
        raise NotImplementedError("crawl discovery not yet implemented")

    # 1. Parse sitemaps
    _echo("Fetching sitemaps...")
    all_urls = await parse_sitemaps(config.sitemaps, config.user_agent)
    _echo(f"  Total URLs in sitemap: {len(all_urls)}")

    if not all_urls:
        _echo("  ERROR: Sitemap returned 0 URLs — aborting to protect existing vectors")
        return 0

    # 2. Classify URLs
    pages, documents, skipped = classify_urls(all_urls, config)
    _echo(
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
    _echo(f"  Existing vectors in Qdrant: {len(existing_hashes)}")

    # Snapshot for stale detection (before we mutate existing_hashes via batches)
    original_existing_urls = set(existing_hashes.keys())

    total_stored = 0
    total_skipped = 0
    # Track successfully processed URLs for stale detection
    all_processed_urls: set[str] = set()

    # 5. Process pages in batches
    discovered_doc_urls: set[str] = set()
    if pages:
        page_url_list = list(pages.keys())
        total_pages = len(page_url_list)
        num_batches = (total_pages - 1) // PAGE_BATCH_SIZE + 1
        _echo(f"\nProcessing {total_pages} web pages ({num_batches} batches)...")

        for batch_start in range(0, total_pages, PAGE_BATCH_SIZE):
            batch_urls = {
                u: pages[u]
                for u in page_url_list[batch_start : batch_start + PAGE_BATCH_SIZE]
            }
            batch_num = batch_start // PAGE_BATCH_SIZE + 1

            page_results, batch_discovered = await fetch_pages(
                batch_urls,
                config,
                counter_offset=batch_start,
                total_override=total_pages,
            )
            discovered_doc_urls.update(batch_discovered)

            items = [
                {
                    "url": p.url,
                    "markdown": p.markdown,
                    "title": p.title,
                    "extra_meta": {
                        "content_type": "page",
                        "document_format": None,
                        "sitemap_lastmod": pages.get(p.url),
                    },
                }
                for p in page_results
            ]

            stored, skipped_batch = _embed_and_store_batch(
                items, existing_hashes, config.name, app_config, force
            )
            total_stored += stored
            total_skipped += skipped_batch
            all_processed_urls.update(p.url for p in page_results)

            _echo(
                f"  Page batch {batch_num}/{num_batches}: "
                f"{stored} vectors stored, {skipped_batch} unchanged"
            )

    # 5b. Merge linked documents discovered from page content
    if discovered_doc_urls and not pages_only:
        new_docs = {u: None for u in discovered_doc_urls if u not in documents}
        if new_docs:
            _echo(
                f"\n  Discovered {len(new_docs)} additional documents "
                f"linked from page content"
            )
            documents.update(new_docs)

    # 6. Process documents (each doc is embedded+stored immediately after conversion)
    if documents:
        # Pre-filter: skip documents already in Qdrant for fast resume.
        # Documents (PDF/DOCX/PPTX) rarely change, so re-downloading just
        # to verify the hash is wasteful.  Use --force to re-check everything.
        if not force:
            already_stored = set(documents.keys()) & set(existing_hashes.keys())
            if already_stored:
                _echo(
                    f"\n  Skipping {len(already_stored)} documents already in Qdrant"
                )
                all_processed_urls.update(already_stored)
                total_skipped += len(already_stored)
                documents = {
                    u: lm for u, lm in documents.items() if u not in already_stored
                }

        if documents:
            cap_note = f" (max {max_new_docs} nya)" if max_new_docs is not None else ""
            _echo(f"\nProcessing {len(documents)} new documents{cap_note}...")

            doc_stored, doc_skipped, doc_urls, consumed_budget = await process_documents(
                documents, config, config.name, app_config, existing_hashes, force,
                max_new_docs=max_new_docs,
            )
            total_stored += doc_stored
            total_skipped += doc_skipped
            all_processed_urls.update(doc_urls)
        else:
            consumed_budget = 0
    else:
        consumed_budget = 0

    # 7. Stale detection (only for full crawls, not filtered by pages/docs-only)
    #    Safety: abort deletion if sitemap looks incomplete (>50% would be removed).
    #    Also skip when max_new_docs was set — a capped run intentionally leaves
    #    unprocessed docs which would otherwise be misclassified as stale.
    STALE_MAX_RATIO = 0.5
    stale_count = 0
    if max_new_docs is not None:
        _echo(
            f"\n  Stale-detection hoppas över: max_new_docs={max_new_docs} "
            f"satt, okompletta körningar kan inte säkert avgöra stale."
        )
    elif not pages_only and not docs_only:
        stale = original_existing_urls - all_processed_urls
        if stale and original_existing_urls:
            ratio = len(stale) / len(original_existing_urls)
            if ratio > STALE_MAX_RATIO:
                _echo(
                    f"\n  WARNING: {len(stale)} stale URLs = {ratio:.0%} of existing vectors"
                    f" (threshold {STALE_MAX_RATIO:.0%}) — skipping deletion."
                    f"\n  This usually means the sitemap was incomplete or the crawl"
                    f" failed partially. Use 'remove-site' to clean up manually if needed."
                )
            else:
                stale_count = len(stale)
                _echo(f"\n  Removing {stale_count} stale URLs ({ratio:.0%} of existing)...")
                delete_by_source_urls(
                    app_config.qdrant_url, app_config.qdrant_api_key, config.name, stale
                )

    _echo(
        f"\n  Done: {total_stored} vectors stored, "
        f"{total_skipped} unchanged, {stale_count} stale removed"
    )

    return consumed_budget
