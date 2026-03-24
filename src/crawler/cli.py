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
