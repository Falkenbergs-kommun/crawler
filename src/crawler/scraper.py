"""Crawl websites with JavaScript rendering using crawl4ai."""

from __future__ import annotations

import asyncio
import re
from dataclasses import dataclass
from urllib.parse import unquote, urljoin, urlparse

import click
from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig
from crawl4ai.markdown_generation_strategy import DefaultMarkdownGenerator

from .config import SiteConfig


@dataclass
class PageResult:
    url: str
    title: str
    markdown: str


async def _crawl_page(crawler: AsyncWebCrawler, url: str) -> tuple[str, str, list[str]]:
    """Crawl a single page. Returns (markdown, title, found_links)."""
    config = CrawlerRunConfig(
        markdown_generator=DefaultMarkdownGenerator(
            content_source="fit_html",
        ),
        wait_until="domcontentloaded",
        page_timeout=60000,
        delay_before_return_html=3.0,  # Wait for JS rendering after DOM load
    )

    result = await crawler.arun(url=url, config=config)

    if not result.success:
        click.echo(f"  Failed to crawl: {url} — {result.error_message}")
        return "", "", []

    markdown = result.markdown.raw_markdown if result.markdown else ""
    title = result.metadata.get("title", "") if result.metadata else ""

    # Extract internal links from the page
    links = []
    if result.links:
        for link_group in [result.links.get("internal", []), result.links.get("external", [])]:
            for link in link_group:
                href = link.get("href", "") if isinstance(link, dict) else str(link)
                if href:
                    links.append(urljoin(url, href))

    return markdown, title, links


def _should_follow(link: str, site: SiteConfig) -> bool:
    """Check if a link should be followed based on site config."""
    parsed = urlparse(link)

    # Skip non-http
    if parsed.scheme not in ("http", "https"):
        return False

    # Skip anchors/fragments-only, mailto, tel
    if not parsed.netloc:
        return False

    # Check allowed domains
    if site.allowed_domains and parsed.netloc not in site.allowed_domains:
        return False

    # Check URL filter
    if site.url_filter and site.url_filter not in link:
        return False

    # Skip common non-content patterns
    skip_patterns = [
        r"\.(pdf|jpg|jpeg|png|gif|svg|css|js|zip|exe|mp4|mp3)(\?|$)",
        r"(login|logout|signin|signup|auth)",
    ]
    for pattern in skip_patterns:
        if re.search(pattern, link, re.IGNORECASE):
            return False

    return True


async def crawl_site(site: SiteConfig) -> list[PageResult]:
    """Crawl a site recursively up to max_depth, rendering JavaScript."""
    visited: set[str] = set()
    results: list[PageResult] = []

    # Normalize starting URL
    start_url = site.url.rstrip("/")

    browser_config = BrowserConfig(
        headless=True,
        browser_type="chromium",
    )

    async with AsyncWebCrawler(config=browser_config) as crawler:
        # BFS crawl
        queue: list[tuple[str, int]] = [(start_url, 0)]

        while queue:
            url, depth = queue.pop(0)

            # Normalize URL for deduplication (decode %C3%A4 → ä etc.)
            normalized = unquote(url.rstrip("/").split("#")[0].split("?")[0])
            if normalized in visited:
                continue
            visited.add(normalized)

            click.echo(f"  [{depth}] Crawling: {url}")

            markdown, title, links = await _crawl_page(crawler, url)

            if markdown.strip():
                results.append(PageResult(url=url, title=title, markdown=markdown))

            # Add child links if we haven't reached max depth
            if depth < site.max_depth:
                for link in links:
                    link_normalized = unquote(link.rstrip("/").split("#")[0].split("?")[0])
                    if link_normalized not in visited and _should_follow(link, site):
                        queue.append((link, depth + 1))

    click.echo(f"  Crawled {len(results)} pages from {site.url}")
    return results
