"""CLI interface for the web crawler."""

from __future__ import annotations

import asyncio
from pathlib import Path

import click

from .chunker import chunk_page, content_hash
from .config import load_config
from .embedder import embed_texts
from .gdrive import extract_google_documents, extract_youtube_metadata
from .scraper import crawl_site
from .store import (
    delete_by_source_urls,
    delete_collection,
    delete_site_from_collection,
    ensure_collection,
    get_existing_hashes,
    list_collections,
    upsert_chunks,
)


@click.group()
@click.option(
    "--config",
    "config_path",
    default="config.yaml",
    help="Path to config.yaml",
    type=click.Path(exists=True),
)
@click.pass_context
def cli(ctx: click.Context, config_path: str) -> None:
    """Crawl websites and index them into Qdrant for RAG."""
    ctx.ensure_object(dict)
    ctx.obj["config_path"] = str(Path(config_path).resolve())


@cli.command()
@click.option("--collection", default=None, help="Only crawl sites for this collection")
@click.option("--force", is_flag=True, help="Force re-embedding of all pages, ignoring content hashes")
@click.pass_context
def crawl(ctx: click.Context, collection: str | None, force: bool) -> None:
    """Crawl websites and store embeddings in Qdrant.

    By default, only new or changed pages are embedded (incremental sync).
    Pages that no longer exist are removed. Use --force to re-embed everything.
    """
    cfg = load_config(ctx.obj["config_path"])

    collections = cfg.collections
    if collection:
        collections = [c for c in collections if c.name == collection]
        if not collections:
            click.echo(f"Collection '{collection}' not found in config.")
            raise SystemExit(1)

    for coll in collections:
        click.echo(f"\n{'='*60}")
        click.echo(f"Collection: {coll.name}")
        click.echo(f"{'='*60}")

        ensure_collection(cfg.qdrant_url, cfg.qdrant_api_key, coll.name)

        for site in coll.sites:
            click.echo(f"\nCrawling: {site.url}")
            pages = asyncio.run(crawl_site(site))

            if not pages:
                click.echo("  No pages found.")
                continue

            # Build {source_url: content_hash} for all crawled content
            crawled_hashes: dict[str, str] = {}
            for page in pages:
                crawled_hashes[page.url] = content_hash(page.markdown)

            # Collect all content: pages + Google Docs + YouTube
            # We store content keyed by source_url so we can diff later
            page_content: dict[str, dict] = {}
            for page in pages:
                page_content[page.url] = {
                    "markdown": page.markdown,
                    "title": page.title,
                    "extra_meta": {},
                }

            # Extract Google Drive/Docs documents
            click.echo("  Checking for Google Drive documents...")
            seen_doc_urls: set[str] = set()
            for page in pages:
                docs = extract_google_documents(page.external_links, page.url, page.raw_html)
                for doc in docs:
                    if doc.source_url in seen_doc_urls:
                        continue
                    seen_doc_urls.add(doc.source_url)
                    crawled_hashes[doc.source_url] = content_hash(doc.text)
                    page_content[doc.source_url] = {
                        "markdown": doc.text,
                        "title": doc.title,
                        "extra_meta": {
                            "content_type": doc.content_type,
                            "linked_from": page.url,
                        },
                    }

            # Extract YouTube video metadata
            click.echo("  Checking for YouTube videos...")
            seen_yt_urls: set[str] = set()
            for page in pages:
                yt_docs = extract_youtube_metadata(page.external_links, page.raw_html)
                for doc in yt_docs:
                    if doc.source_url in seen_yt_urls:
                        continue
                    seen_yt_urls.add(doc.source_url)
                    crawled_hashes[doc.source_url] = content_hash(doc.text)
                    page_content[doc.source_url] = {
                        "markdown": doc.text,
                        "title": doc.title,
                        "extra_meta": {
                            "content_type": "youtube_video",
                            "linked_from": page.url,
                        },
                    }

            # Diff against existing hashes in Qdrant
            existing_hashes = get_existing_hashes(
                cfg.qdrant_url, cfg.qdrant_api_key, coll.name, site.url
            )

            all_crawled_urls = set(crawled_hashes.keys())
            all_existing_urls = set(existing_hashes.keys())

            if force:
                changed_or_new = all_crawled_urls
                unchanged: set[str] = set()
            else:
                unchanged = {
                    url for url in all_crawled_urls & all_existing_urls
                    if crawled_hashes[url] == existing_hashes[url]
                }
                changed_or_new = all_crawled_urls - unchanged
            stale = all_existing_urls - all_crawled_urls

            click.echo(f"  {len(all_crawled_urls)} URLs crawled: "
                        f"{len(unchanged)} unchanged, {len(changed_or_new)} new/changed, "
                        f"{len(stale)} stale")

            # Delete stale URLs (pages that no longer exist)
            # Safety: refuse to delete if >50% would be removed — likely a crawl failure
            STALE_MAX_RATIO = 0.5
            if stale and all_existing_urls:
                ratio = len(stale) / len(all_existing_urls)
                if ratio > STALE_MAX_RATIO:
                    click.echo(
                        f"  WARNING: {len(stale)} stale URLs = {ratio:.0%} of existing"
                        f" (threshold {STALE_MAX_RATIO:.0%}) — skipping deletion."
                        f"\n  Likely a crawl failure. Use 'remove-site' to clean up manually."
                    )
                    stale = set()  # prevent deletion below
            if stale:
                click.echo(f"  Removing {len(stale)} stale URLs...")
                delete_by_source_urls(
                    cfg.qdrant_url, cfg.qdrant_api_key, coll.name, stale
                )

            if not changed_or_new:
                click.echo("  Nothing to update.")
                continue

            # Delete old vectors for changed URLs before re-upserting
            changed_existing = changed_or_new & all_existing_urls
            if changed_existing:
                delete_by_source_urls(
                    cfg.qdrant_url, cfg.qdrant_api_key, coll.name, changed_existing
                )

            # Chunk only changed/new content
            all_chunks = []
            for source_url in sorted(changed_or_new):
                content = page_content[source_url]
                chunks = chunk_page(
                    markdown=content["markdown"],
                    source_url=source_url,
                    page_title=content["title"],
                    site_name=site.url,
                )
                for c in chunks:
                    c.metadata.update(content["extra_meta"])
                all_chunks.extend(chunks)

            click.echo(f"  {len(all_chunks)} chunks to embed")

            if not all_chunks:
                continue

            # Embed
            texts = [c.text for c in all_chunks]
            click.echo("  Generating embeddings...")
            embeddings = embed_texts(texts, cfg.openai_api_key)

            # Store
            click.echo("  Storing in Qdrant...")
            count = upsert_chunks(
                cfg.qdrant_url, cfg.qdrant_api_key, coll.name, all_chunks, embeddings
            )
            click.echo(f"  Stored {count} vectors in '{coll.name}'")

    click.echo("\nDone.")


@cli.command("crawl-external")
@click.option("--site", default=None, help="Only crawl this external site (by name)")
@click.option("--force", is_flag=True, help="Force re-embedding, ignore hashes and lastmod")
@click.option("--pages-only", is_flag=True, help="Only crawl web pages, skip documents")
@click.option("--docs-only", is_flag=True, help="Only crawl documents, skip web pages")
@click.option(
    "--max-new-docs",
    type=int,
    default=None,
    help="Max antal NYA dokument att Docling-konvertera totalt över alla sites (successer + timeouts + fel). Ingen begränsning om utelämnad.",
)
@click.pass_context
def crawl_external(
    ctx: click.Context,
    site: str | None,
    force: bool,
    pages_only: bool,
    docs_only: bool,
    max_new_docs: int | None,
) -> None:
    """Crawl external websites defined in config and index to Qdrant."""
    from .external import crawl_external_site

    cfg = load_config(ctx.obj["config_path"])

    if not cfg.external_sites:
        click.echo("No external_sites defined in config.")
        raise SystemExit(1)

    sites = cfg.external_sites
    if site:
        sites = [s for s in sites if s.name == site]
        if not sites:
            click.echo(f"External site '{site}' not found in config.")
            raise SystemExit(1)

    if pages_only and docs_only:
        click.echo("Cannot use --pages-only and --docs-only together.")
        raise SystemExit(1)

    remaining_budget = max_new_docs  # global budget across sites; None = unlimited

    for ext_site in sites:
        click.echo(f"\n{'='*60}")
        click.echo(f"External site: {ext_site.name}")
        if remaining_budget is not None:
            click.echo(f"Kvarvarande doc-budget: {remaining_budget}")
        click.echo(f"{'='*60}")

        if remaining_budget is not None and remaining_budget <= 0:
            click.echo("Budget slut — hoppar över resterande sites.")
            break

        consumed = asyncio.run(crawl_external_site(
            config=ext_site,
            app_config=cfg,
            force=force,
            pages_only=pages_only,
            docs_only=docs_only,
            max_new_docs=remaining_budget,
        ))

        if remaining_budget is not None:
            remaining_budget = max(0, remaining_budget - (consumed or 0))

    click.echo("\nDone.")


@cli.command("list")
@click.pass_context
def list_cmd(ctx: click.Context) -> None:
    """List all Qdrant collections."""
    cfg = load_config(ctx.obj["config_path"])
    collections = list_collections(cfg.qdrant_url, cfg.qdrant_api_key)

    if not collections:
        click.echo("No collections found.")
        return

    click.echo(f"{'Name':<30} {'Points':<10} {'Dimensions':<10}")
    click.echo("-" * 50)
    for c in collections:
        click.echo(f"{c['name']:<30} {c['points']:<10} {c['vectors_size']:<10}")


@cli.command()
@click.option("--collection", required=True, help="Collection to delete")
@click.confirmation_option(prompt="Are you sure you want to delete this collection?")
@click.pass_context
def delete(ctx: click.Context, collection: str) -> None:
    """Delete a Qdrant collection."""
    cfg = load_config(ctx.obj["config_path"])
    if delete_collection(cfg.qdrant_url, cfg.qdrant_api_key, collection):
        click.echo(f"Deleted collection '{collection}'.")
    else:
        click.echo(f"Collection '{collection}' not found.")


@cli.command("crawl-intranet")
@click.option("--collection", default=None, help="Kör bara denna intranet-collection")
@click.option("--article-id", type=int, default=None, help="Kör bara en specifik artikel (test)")
@click.option("--force", is_flag=True, help="Tvinga om-embedding, ignorera hash")
@click.pass_context
def crawl_intranet(
    ctx: click.Context,
    collection: str | None,
    article_id: int | None,
    force: bool,
) -> None:
    """Hämta intranet-artiklar (Joomla) via DB och indexera till Qdrant."""
    from .intranet import crawl_intranet_articles

    cfg = load_config(ctx.obj["config_path"])

    if not cfg.intranet_articles:
        click.echo("Inga intranet_articles definierade i config.")
        raise SystemExit(1)

    if cfg.intranet_db is None:
        click.echo("INTRANET_DB_HOST saknas i .env — kan inte ansluta till intranätets DB.")
        raise SystemExit(1)

    asyncio.run(crawl_intranet_articles(
        articles_config=cfg.intranet_articles,
        app_config=cfg,
        only_collection=collection,
        only_article_id=article_id,
        force=force,
    ))

    click.echo("\nKlart.")


@cli.command("crawl-single-pages")
@click.option("--collection", default=None, help="Kör bara denna single-pages-collection")
@click.option("--url", "url_filter", default=None, help="Kör bara denna specifika URL (test)")
@click.option("--force", is_flag=True, help="Tvinga om-embedding, ignorera hash")
@click.pass_context
def crawl_single_pages_cmd(
    ctx: click.Context,
    collection: str | None,
    url_filter: str | None,
    force: bool,
) -> None:
    """Hämta enstaka webbsidor (utan crawling) och indexera till Qdrant."""
    from .single_pages import crawl_single_pages

    cfg = load_config(ctx.obj["config_path"])

    if not cfg.single_pages:
        click.echo("Inga single_pages definierade i config.")
        raise SystemExit(1)

    asyncio.run(crawl_single_pages(
        pages_config=cfg.single_pages,
        app_config=cfg,
        only_collection=collection,
        only_url=url_filter,
        force=force,
    ))

    click.echo("\nKlart.")


@cli.command("sync-config")
@click.option(
    "--sheet-id",
    default="1D_i7tHPdEPQ1giXCZIQormZNK_hAX3vkQBuIFRvxubw",
    help="Google Sheet ID (defaultar till sources-arket)",
)
@click.option("--gid", default=0, type=int, help="Sheet tab gid (default: 0)")
@click.option(
    "--apply",
    "do_apply",
    is_flag=True,
    help="Skriv till config.yaml. Utan denna flagga körs endast dry-run med diff.",
)
@click.pass_context
def sync_config(ctx: click.Context, sheet_id: str, gid: int, do_apply: bool) -> None:
    """Synka källor från Google-ark till config.yaml.

    Dry-run är default. Använd --apply för att faktiskt skriva ändringar.
    Tekniska fält (max_depth, sitemap, ocr, etc.) lämnas orörda på befintliga
    entries; endast nya källor läggs till. Orphans (i config men inte i ark)
    flaggas men raderas aldrig automatiskt.
    """
    from .sheet_sync import apply_diff, compute_diff, fetch_sheet_rows, render_diff

    cfg = load_config(ctx.obj["config_path"])
    config_path = Path(ctx.obj["config_path"])

    click.echo(f"Läser sheet {sheet_id[:16]}… (gid={gid})...")
    try:
        rows = fetch_sheet_rows(sheet_id, gid)
    except Exception as e:
        click.echo(f"  FEL: Kunde inte läsa ark — {e}")
        raise SystemExit(1)

    click.echo(f"  {len(rows)} rader lästa.")
    if not rows:
        click.echo("  ERROR: Arket är tomt — aborterar (skyddar befintlig config).")
        raise SystemExit(1)
    click.echo("")

    diff = compute_diff(rows, cfg)
    click.echo(render_diff(diff))
    click.echo("")

    if not diff.has_changes():
        click.echo("Inga ändringar att skriva.")
        return

    if not do_apply:
        click.echo("Kör med --apply för att skriva ändringar till config.yaml.")
        return

    apply_diff(diff, config_path)
    click.echo(f"Skrev ändringar till {config_path}.")


@cli.command("remove-site")
@click.option("--collection", required=True, help="Collection name")
@click.option("--url", "site_url", required=True, help="Site URL to remove")
@click.pass_context
def remove_site(ctx: click.Context, collection: str, site_url: str) -> None:
    """Remove all vectors from a specific site within a collection."""
    cfg = load_config(ctx.obj["config_path"])
    delete_site_from_collection(
        cfg.qdrant_url, cfg.qdrant_api_key, collection, site_url
    )
    click.echo(f"Removed vectors for '{site_url}' from '{collection}'.")
