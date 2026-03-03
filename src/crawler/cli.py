"""CLI interface for the web crawler."""

from __future__ import annotations

import asyncio
from pathlib import Path

import click

from .chunker import chunk_page
from .config import load_config
from .embedder import embed_texts
from .gdrive import extract_google_documents, extract_youtube_metadata
from .scraper import crawl_site
from .store import (
    delete_collection,
    delete_site_from_collection,
    ensure_collection,
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
@click.pass_context
def crawl(ctx: click.Context, collection: str | None) -> None:
    """Crawl websites and store embeddings in Qdrant."""
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

            # Chunk all crawled pages
            all_chunks = []
            for page in pages:
                chunks = chunk_page(
                    markdown=page.markdown,
                    source_url=page.url,
                    page_title=page.title,
                    site_name=site.url,
                )
                all_chunks.extend(chunks)

            # Extract Google Drive/Docs documents linked from pages
            click.echo("  Checking for Google Drive documents...")
            seen_doc_urls: set[str] = set()
            for page in pages:
                docs = extract_google_documents(page.external_links, page.url, page.raw_html)
                for doc in docs:
                    if doc.source_url in seen_doc_urls:
                        continue
                    seen_doc_urls.add(doc.source_url)
                    doc_chunks = chunk_page(
                        markdown=doc.text,
                        source_url=doc.source_url,
                        page_title=doc.title,
                        site_name=site.url,
                    )
                    # Tag chunks with content type
                    for c in doc_chunks:
                        c.metadata["content_type"] = doc.content_type
                        c.metadata["linked_from"] = page.url
                    all_chunks.extend(doc_chunks)

            # Extract YouTube video metadata
            click.echo("  Checking for YouTube videos...")
            seen_yt_urls: set[str] = set()
            for page in pages:
                yt_docs = extract_youtube_metadata(page.external_links, page.raw_html)
                for doc in yt_docs:
                    if doc.source_url in seen_yt_urls:
                        continue
                    seen_yt_urls.add(doc.source_url)
                    doc_chunks = chunk_page(
                        markdown=doc.text,
                        source_url=doc.source_url,
                        page_title=doc.title,
                        site_name=site.url,
                    )
                    for c in doc_chunks:
                        c.metadata["content_type"] = "youtube_video"
                        c.metadata["linked_from"] = page.url
                    all_chunks.extend(doc_chunks)

            click.echo(f"  {len(all_chunks)} chunks from {len(pages)} pages")

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
