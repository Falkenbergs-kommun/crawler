# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Web crawler that indexes Swedish municipality Google Sites and external websites into Qdrant vector collections for RAG. It crawls JS-rendered pages, extracts linked Google Docs/Sheets/Slides/Drive PDFs and YouTube metadata, chunks the content, generates OpenAI embeddings, and upserts to Qdrant. External websites (e.g. Skolverket) are discovered via sitemap parsing and use httpx + trafilatura for pages and Docling for documents (PDF/DOCX/PPTX).

## Setup

```bash
# Install Python dependencies (uses uv, not pip)
uv sync

# Install Playwright's Chromium browser
uv run playwright install chromium
```

### System libraries (no sudo required)

Playwright's Chromium needs system libraries (libnspr4, libnss3, etc.) that may not be
installed on the server. Since we don't have sudo, we download the .deb packages and
extract the .so files into a local directory:

```bash
mkdir -p .local-libs /tmp/deb-extract
cd /tmp/deb-extract
apt-get download libnspr4 libnss3 libatk1.0-0 libatk-bridge2.0-0 libatspi2.0-0 libxcomposite1 libxdamage1
for deb in *.deb; do dpkg-deb -x "$deb" extracted/; done
cp extracted/usr/lib/x86_64-linux-gnu/*.so* /path/to/crawler/.local-libs/
rm -rf /tmp/deb-extract
```

Then add this line to `.env` so the crawler finds them automatically:

```
LD_LIBRARY_PATH=/absolute/path/to/.local-libs
```

Verify with: `ldd ~/.cache/ms-playwright/chromium-*/chrome-linux64/chrome 2>&1 | grep "not found"`
— should return nothing.

**Updating these libraries:** These are stable C libraries (NSS, NSPR, ATK, X11) that
rarely need patching. They should be updated if Playwright upgrades its bundled Chromium
to a version that requires newer .so versions — this would show up as a missing symbol
error or version mismatch when launching the crawler. To update, re-run the `apt-get
download` commands above to get the latest versions from the Debian repos.

## Commands

```bash
# Run the crawler (incremental — only embeds new/changed pages)
uv run crawler crawl

# Force re-embedding of all pages (ignores content hashes)
uv run crawler crawl --force

# Crawl a single collection
uv run crawler crawl --collection fokus-ai

# List collections with point counts
uv run crawler list

# Delete a collection (requires confirmation)
uv run crawler delete --collection <name>

# Remove a single site's vectors from a collection
uv run crawler remove-site --collection <name> --url <site-url>

# Use a different config file
uv run crawler --config path/to/config.yaml crawl

# --- External websites (sitemap-based) ---

# Crawl all external sites (pages + documents)
uv run crawler crawl-external

# Crawl only web pages for a specific site
uv run crawler crawl-external --site skolverket --pages-only

# Crawl only documents (PDF/DOCX/PPTX) — heavy, run with nohup
nohup uv run crawler crawl-external --site skolverket --docs-only > crawl-docs.log 2>&1 &

# Force re-embedding of all content
uv run crawler crawl-external --site skolverket --force

# Cap new-doc processing (nightly-friendly, stops after N Docling attempts)
uv run crawler crawl-external --docs-only --max-new-docs 50

# --- Intranet articles (Joomla, DB-backed) ---

# Fetch all configured intranet articles (reads ez3pg_content directly)
uv run crawler crawl-intranet

# Test a single article
uv run crawler crawl-intranet --article-id 3419

# --- Single pages (one URL, no crawling) ---
uv run crawler crawl-single-pages

# --- Sync source list from editorial Google Sheet ---

# Dry-run (shows diff, does NOT modify config.yaml)
uv run crawler sync-config

# Apply changes
uv run crawler sync-config --apply

# --- Nightly automation ---

# Wrapper script used by cron — runs sync-config + all fast pipelines + capped docs
bin/nightly-sync.sh
```

No test suite exists yet.

## Architecture

The pipeline flows: **crawl → chunk → embed → store**, orchestrated by `cli.py`.

- **cli.py** — Click CLI. The `crawl` command does incremental sync: crawls all pages, compares content hashes (SHA-256) against Qdrant, and only embeds new/changed pages. Stale pages (no longer in crawl results) are deleted (with a 50% safety threshold — see below). Use `--force` to skip hash comparison and re-embed everything.
- **scraper.py** — Async BFS crawler using crawl4ai with Playwright/Chromium. Uses `fit_html` content source for Google Sites main content extraction. URL deduplication via URL-decoded normalization. Returns `PageResult` with markdown, raw HTML, and separated internal/external links.
- **gdrive.py** — Extracts Google Docs/Sheets/Slides/Drive files and YouTube metadata from external links AND raw HTML (since crawl4ai often misses Drive links). Downloads via public export URLs (no auth needed for published docs).
- **chunker.py** — Token-aware splitting with `langchain-text-splitters` using `cl100k_base` tokenizer (512 tokens, 100 overlap). Each chunk carries metadata: source_url, page_title, site_name, chunk_index, crawl_date, content_hash.
- **embedder.py** — OpenAI `text-embedding-3-large` (3072 dimensions), batched in 100s with exponential backoff retry.
- **store.py** — Qdrant client wrapper. Uses deterministic UUIDs (`uuid5` from url+chunk_index) for idempotent upserts. Creates payload indexes on `source_url` and `site_name` for filtered queries and site-level deletion. `get_existing_hashes()` scrolls Qdrant to retrieve stored content hashes for diff comparison. `url_exists_in_qdrant()` does a lightweight single-URL existence check for live deduplication during parallel runs.
- **config.py** — Loads `config.yaml` (collections, sites, external_sites, intranet_articles, single_pages) + `.env` (API keys, intranet DB creds). Config is relative to the YAML file location. Dataclasses: `SiteConfig`, `CollectionConfig`, `ExternalSiteConfig`, `IntranetArticleConfig`, `SinglePageConfig`, `IntranetDBConfig`.
- **sheet_sync.py** — Syncs source definitions from a Google Sheet into `config.yaml`. `fetch_sheet_rows()` auto-detects the header row (sheet may have title/empty rows first). `classify_row()` maps each row to one of: `intranet` (Kommentarer=Intranätsida + Artikel-ID), `google_sites` (URL under sites.google.com), `single_page` (URL, Följ länkar≠JA), `external_site` (URL, Följ länkar=JA — flagged as needing manual sitemap config), or `skip`. `compute_diff()` matches existing config entries by URL-path-prefix for Google Sites, article_id for intranet, exact URL for single pages — preserves technical fields (max_depth, sitemap, ocr, etc.) untouched. `apply_diff()` uses `ruamel.yaml` round-trip to preserve comments, quotes, and ordering. Additive-only: orphans (in config, not in sheet) are flagged but not removed.
- **intranet.py** — Crawl Joomla intranet articles by ID via direct DB read (`<prefix>content.introtext` + `fulltext`). Bypasses SSO/login. HTML cleaned with trafilatura, then handed to the shared `_embed_and_store_batch`. `source_url` deterministic: `<base>/index.php?option=com_content&view=article&id=<id>`. Articles grouped by `collection` field; each collection becomes its own Qdrant collection.
- **single_pages.py** — Fetch standalone URLs (no crawling) via httpx + trafilatura. One `SinglePageConfig` entry per URL. Reuses `_embed_and_store_batch` so hash-diff and stale-detection behave identically to external pages.
- **external.py** — Pipeline for external websites: sitemap parsing (with gzip/sitemap-index support), URL classification, page fetching (httpx + trafilatura), document processing (httpx + Docling), and orchestration with incremental sync. Documents are processed strictly one at a time in a sequential loop to keep memory bounded — all intermediate data (PDF bytes, text, chunks, embeddings) is released between documents. Docling conversion runs in a subprocess (`ProcessPoolExecutor`) so that timeouts actually kill the worker — thread-based timeouts left zombie threads that caused OOM kills. Pages are still processed in parallel batches (200) since they are lightweight. All output uses `_echo()` which flushes stdout for nohup compatibility. When `discover_linked_documents` is enabled, `fetch_pages` also scans page HTML for `<a href>` links to documents (matching `document_extensions`) and feeds discovered URLs into the document processing stage — this catches documents linked from page content but not listed in the sitemap (e.g. external PDFs on other domains).
- **docling_utils.py** — Shared lazy-loaded Docling `DocumentConverter` instances, cached per OCR setting. When `ocr=False`, Docling skips bitmap/OCR processing and only extracts programmatically embedded text — much faster for born-digital PDFs. Used by `gdrive.py` (in-process) and `external.py` (in subprocess workers).

## Key Gotchas

- **Qdrant client ignores port in URL** — `store.py:_make_client` parses the URL manually and passes `host`/`port`/`https` separately.
- **IPv4 forced globally** — `store.py` patches `socket.getaddrinfo` because the Qdrant server has AAAA DNS records but doesn't actually serve IPv6. This affects the entire process.
- **Google Sites timeouts** — Some pages are JS-heavy; `page_timeout` is set to 60s with a 3s `delay_before_return_html` for rendering.
- **Content from multiple sources** — A single site crawl produces chunks from: the page itself, linked Google Docs/Sheets/Slides, Drive PDFs, and YouTube video metadata. The `content_type` and `linked_from` metadata fields distinguish these.

## Key Gotchas (External)

- **Docling is CPU-heavy** — Documents are processed sequentially (one at a time) to keep memory bounded. Docling's layout analysis (table detection CNN) runs even with OCR disabled and takes 30-200+ seconds per PDF on CPU. A 600-second timeout kills conversions that hang.
- **Docling runs in subprocess** — `external.py` uses `ProcessPoolExecutor(max_workers=1)` for Docling conversion. This ensures timeouts actually terminate the worker process. Previous approach used `asyncio.to_thread` where timeouts only cancelled the coroutine while the underlying thread kept running — accumulating zombie threads that caused OOM kills (~15 GB RSS). On timeout or worker crash (`BrokenProcessPool`), the pool is shut down and recreated so subsequent documents can still be processed.
- **OCR can be disabled per site** — Set `ocr: false` in `config.yaml` for sites with born-digital PDFs (e.g. Skolverket). This skips Docling's RapidOCR stage but layout analysis still runs. Docling with OCR is kept for `gdrive.py` (Google Drive PDFs may be scanned).
- **stdout buffering with nohup** — All output in `external.py` goes through `_echo()` which calls `click.echo()` + explicit flush. Without this, output is fully buffered when redirected to a file via nohup, making logs appear empty during long runs.
- **Documents are embedded immediately** — Each document is chunked, embedded, and stored in Qdrant right after Docling conversion. This enables resume on interruption (already-stored docs are detected via content hash and skipped on restart) and provides real-time progress in logs.
- **Stale detection disabled for partial crawls** — When using `--pages-only` or `--docs-only`, stale vectors are not deleted to avoid removing vectors belonging to the other content type.
- **Stale deletion safety threshold** — Both `cli.py` and `external.py` refuse to delete stale vectors if >50% of existing vectors would be removed (`STALE_MAX_RATIO = 0.5`). This protects against mass deletion caused by sitemap outages, crawl failures, or incomplete results. Use `remove-site` for manual cleanup if needed.
- **Empty sitemap abort** — `external.py` aborts immediately if the sitemap returns 0 URLs, preventing stale detection from treating all existing vectors as stale.
- **Per-document live Qdrant check** — During document processing, each document is checked against Qdrant before download (`url_exists_in_qdrant`). This enables safe parallel `--docs-only` runs on multiple servers — the second instance skips documents already stored by the first, avoiding redundant Docling conversion and embedding costs.
- **Sitemap gzip detection** — Uses URL suffix `.gz`, `Content-Type`, or `Content-Encoding` headers. `httpx` auto-decompresses `Content-Encoding: gzip`, but sitemap files served as `application/gzip` need manual `gzip.decompress()`.
- **Linked document discovery** — When `discover_linked_documents: true` in config, `fetch_pages` scans every fetched HTML page for `<a href>` links matching `document_extensions`. Discovered URLs (including cross-domain) are merged into the document processing queue. This is opt-in per site to avoid unexpected Docling load on sites where the sitemap already covers all documents. Discovered docs get `lastmod: None` but incremental sync still works via content hashing.
- **Skip list for failing documents** — Timeouts and convert errors append the URL to `skip/<site>.tsv`. Future runs check the skip list before download and skip known failures. This matters because Docling timeouts are usually stable per document (corrupt or unusually complex PDF) — retrying costs another 600s per doc without fixing anything. To retry a specific document, delete its line from the TSV.
- **Per-night document budget** — `--max-new-docs N` caps how many documents reach Docling conversion (successes + timeouts + convert failures; download errors and Qdrant hits don't count). Budget is global across all external sites in one invocation. When a budget is set, stale-detection is skipped — an intentionally truncated run cannot reliably distinguish "processed" from "not yet reached" and could otherwise delete untouched vectors.

## Nightly automation

`bin/nightly-sync.sh` is a bash wrapper that runs in cron (02:15 daily):

1. Pings Healthchecks `/start`
2. Runs in sequence: `sync-config --apply` → `crawl` (Google Sites) → `crawl-intranet` → `crawl-single-pages` → `crawl-external --docs-only --max-new-docs 50`
3. Captures stdout+stderr per step, extracts per-step summary (stored/unchanged counts)
4. Builds a top-of-body summary with stage status + key metrics
5. POSTs full log as body to `/ping/<UUID>` (success) or `/ping/<UUID>/fail` (any step failed)

`crawl-external-docs` in the nightly uses `--docs-only` to skip the pages phase (3979+ URLs for Skolverket re-fetched every night would be too heavy). Pages can be scheduled separately (weekly recommended).

## Configuration

- `config.yaml` — Defines four source types. `collections` (Google Sites crawled with Playwright). `external_sites` (sitemap-based, httpx + Docling). `intranet_articles` (Joomla articles by ID, direct DB read; `collection` field groups related articles). `single_pages` (one URL per entry, httpx + trafilatura).
- `.env` — `OPENAI_API_KEY` (required). `QDRANT_URL`, `QDRANT_API_KEY` (Qdrant). `INTRANET_DB_HOST/NAME/USER/PASS/PREFIX`, `INTRANET_BASE_URL` (for `crawl-intranet`, reused from mail-reminders/.env pattern). `HEALTHCHECKS_BASE_URL`, `HEALTHCHECKS_CRAWLER_NIGHTLY_ID` (ping endpoint for nightly wrapper).
- `skip/<site>.tsv` — Persistent skip list per external site. Format: `url<TAB>filename<TAB>error<TAB>date`. Managed automatically by `external.py`; manual edits are respected (delete a row to retry that doc).
