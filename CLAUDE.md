# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Web crawler that indexes Swedish municipality Google Sites into Qdrant vector collections for RAG. It crawls JS-rendered pages, extracts linked Google Docs/Sheets/Slides/Drive PDFs and YouTube metadata, chunks the content, generates OpenAI embeddings, and upserts to Qdrant.

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
```

No test suite exists yet.

## Architecture

The pipeline flows: **crawl → chunk → embed → store**, orchestrated by `cli.py`.

- **cli.py** — Click CLI. The `crawl` command does incremental sync: crawls all pages, compares content hashes (SHA-256) against Qdrant, and only embeds new/changed pages. Stale pages (no longer in crawl results) are deleted. Use `--force` to skip hash comparison and re-embed everything.
- **scraper.py** — Async BFS crawler using crawl4ai with Playwright/Chromium. Uses `fit_html` content source for Google Sites main content extraction. URL deduplication via URL-decoded normalization. Returns `PageResult` with markdown, raw HTML, and separated internal/external links.
- **gdrive.py** — Extracts Google Docs/Sheets/Slides/Drive files and YouTube metadata from external links AND raw HTML (since crawl4ai often misses Drive links). Downloads via public export URLs (no auth needed for published docs).
- **chunker.py** — Token-aware splitting with `langchain-text-splitters` using `cl100k_base` tokenizer (512 tokens, 100 overlap). Each chunk carries metadata: source_url, page_title, site_name, chunk_index, crawl_date, content_hash.
- **embedder.py** — OpenAI `text-embedding-3-large` (3072 dimensions), batched in 100s with exponential backoff retry.
- **store.py** — Qdrant client wrapper. Uses deterministic UUIDs (`uuid5` from url+chunk_index) for idempotent upserts. Creates payload indexes on `source_url` and `site_name` for filtered queries and site-level deletion. `get_existing_hashes()` scrolls Qdrant to retrieve stored content hashes for diff comparison.
- **config.py** — Loads `config.yaml` (collections and sites) + `.env` (API keys). Config is relative to the YAML file location.

## Key Gotchas

- **Qdrant client ignores port in URL** — `store.py:_make_client` parses the URL manually and passes `host`/`port`/`https` separately.
- **IPv4 forced globally** — `store.py` patches `socket.getaddrinfo` because the Qdrant server has AAAA DNS records but doesn't actually serve IPv6. This affects the entire process.
- **Google Sites timeouts** — Some pages are JS-heavy; `page_timeout` is set to 60s with a 3s `delay_before_return_html` for rendering.
- **Content from multiple sources** — A single site crawl produces chunks from: the page itself, linked Google Docs/Sheets/Slides, Drive PDFs, and YouTube video metadata. The `content_type` and `linked_from` metadata fields distinguish these.

## Configuration

- `config.yaml` — Defines collections, each with one or more sites (url, max_depth, allowed_domains, url_filter)
- `.env` — Must contain `OPENAI_API_KEY`. Optionally `QDRANT_URL` (defaults to localhost:6333) and `QDRANT_API_KEY`.
