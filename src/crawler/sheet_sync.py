"""Sync source definitions from a Google Sheet into config.yaml.

The sheet is the editorial source of truth for *which* sources exist;
config.yaml retains technical settings (sitemap URLs, rate limits, OCR).
"""

from __future__ import annotations

import csv
import io
import os
import re
import tempfile
import unicodedata
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal
from urllib.parse import urlparse

import click
import httpx
from ruamel.yaml import YAML
from ruamel.yaml.comments import CommentedMap, CommentedSeq

from .config import AppConfig

SHEET_EXPECTED_COLUMNS = {
    "Display name",
    "URL",
    "Kommentarer",
    "Artikel-ID",
    "Följ länkar",
    "Följ externa länkar",
}

RowKind = Literal["intranet", "google_sites", "single_page", "external_site", "skip"]


# ---------------------------------------------------------------------------
# Fetch
# ---------------------------------------------------------------------------


def fetch_sheet_rows(sheet_id: str, gid: int = 0) -> list[dict[str, str]]:
    """Download a public Google Sheet tab as CSV and parse to dicts.

    The sheet may have title/empty rows before the real header; this scans
    for the first row containing "Display name" and treats that as the header.
    Raises on HTTP errors or missing required columns.
    """
    url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid={gid}"
    resp = httpx.get(url, follow_redirects=True, timeout=30)
    resp.raise_for_status()

    all_rows = list(csv.reader(io.StringIO(resp.text)))
    header_idx = next(
        (i for i, r in enumerate(all_rows) if any(c.strip() == "Display name" for c in r)),
        None,
    )
    if header_idx is None:
        raise ValueError("Could not find header row containing 'Display name' in sheet")

    header = [c.strip() for c in all_rows[header_idx]]
    got = {c for c in header if c}
    missing = SHEET_EXPECTED_COLUMNS - got
    if missing:
        raise ValueError(
            f"Sheet is missing expected columns: {sorted(missing)}. "
            f"Got: {sorted(got)}"
        )

    rows: list[dict[str, str]] = []
    for raw in all_rows[header_idx + 1 :]:
        if not any(cell.strip() for cell in raw):
            continue  # skip empty rows
        # Pad/truncate to header length
        padded = list(raw) + [""] * (len(header) - len(raw))
        rows.append({header[i]: (padded[i] or "").strip() for i in range(len(header)) if header[i]})
    return rows


# ---------------------------------------------------------------------------
# Classification
# ---------------------------------------------------------------------------


def _slugify(text: str) -> str:
    """ASCII-fold + lowercase + spaces to hyphens. 'Fokus AI' → 'fokus-ai'."""
    folded = unicodedata.normalize("NFKD", text)
    folded = folded.encode("ascii", "ignore").decode("ascii")
    folded = folded.lower()
    folded = re.sub(r"[^a-z0-9]+", "-", folded).strip("-")
    return folded or "unnamed"


def _google_sites_path_key(url: str) -> str:
    """Canonical site identifier: first two path segments.

    https://sites.google.com/falkenberg.se/unikumguider/        → '/falkenberg.se/unikumguider'
    https://sites.google.com/falkenberg.se/fokus-ai/startsida   → '/falkenberg.se/fokus-ai'

    Two URLs with different trailing paths but the same site map to the same
    key, which is what makes matching config ↔ sheet robust.
    """
    parsed = urlparse(url)
    parts = [p for p in parsed.path.split("/") if p]
    if len(parts) >= 2:
        return f"/{parts[0]}/{parts[1]}"
    return parsed.path.rstrip("/")


def _google_sites_slug(url: str) -> str:
    """Derive collection slug from a Google Sites URL.

    https://sites.google.com/falkenberg.se/fokus-ai/startsida → 'fokus-ai'
    """
    parsed = urlparse(url)
    parts = [p for p in parsed.path.split("/") if p]
    # Expected path: /<domain>/<sitename>/...  → take index 1
    if len(parts) >= 2:
        return _slugify(parts[1])
    return _slugify(parts[-1] if parts else url)


def _intranet_collection_slug(display_name: str) -> str:
    """Group intranet articles by first word of display name.

    'Digitalguiden startsida' → 'digitalguiden'
    'Digitalguiden uppdrag' → 'digitalguiden'
    """
    first_word = display_name.split()[0] if display_name.strip() else "intranet"
    return _slugify(first_word)


def _yesno(value: str) -> bool:
    return (value or "").strip().upper() in ("JA", "YES", "TRUE", "1")


def classify_row(row: dict[str, str]) -> tuple[RowKind, dict]:
    """Classify a sheet row and return (kind, parsed_attributes).

    The parsed_attributes shape depends on kind:
      intranet:     {collection, article_id, display_name, follow_links, follow_external_links}
      google_sites: {collection, url, display_name}
      single_page:  {collection, url, display_name}
      external_site:{url, display_name}  (warning — needs manual config)
      skip:         {reason}
    """
    display = (row.get("Display name") or "").strip()
    url = (row.get("URL") or "").strip()
    kommentarer = (row.get("Kommentarer") or "").strip()
    article_id_raw = (row.get("Artikel-ID") or "").strip()

    # Intranet article (no URL, has Artikel-ID)
    if kommentarer.lower() == "intranätsida" and article_id_raw:
        try:
            article_id = int(article_id_raw)
        except ValueError:
            return "skip", {"reason": f"Invalid Artikel-ID: {article_id_raw!r}"}
        return "intranet", {
            "collection": _intranet_collection_slug(display),
            "article_id": article_id,
            "display_name": display,
            "follow_links": _yesno(row.get("Följ länkar", "")),
            "follow_external_links": _yesno(row.get("Följ externa länkar", "")),
        }

    if not url:
        return "skip", {"reason": "No URL and no Artikel-ID"}

    if url.startswith("https://sites.google.com/") or url.startswith("http://sites.google.com/"):
        return "google_sites", {
            "collection": _google_sites_slug(url),
            "url": url,
            "display_name": display,
        }

    # External URL — split by Följ länkar
    follow = row.get("Följ länkar", "")
    if _yesno(follow):
        # Wants full crawl — needs manual external_sites setup (sitemap etc.)
        return "external_site", {"url": url, "display_name": display}

    # Single page (Nej or blank)
    return "single_page", {
        "collection": _slugify(display),
        "url": url,
        "display_name": display,
    }


# ---------------------------------------------------------------------------
# Diff
# ---------------------------------------------------------------------------


@dataclass
class SyncDiff:
    google_sites_add: list[dict] = field(default_factory=list)
    google_sites_existing: list[dict] = field(default_factory=list)
    google_sites_orphan: list[str] = field(default_factory=list)

    intranet_add: list[dict] = field(default_factory=list)
    intranet_existing: list[dict] = field(default_factory=list)
    intranet_orphan: list[dict] = field(default_factory=list)

    single_pages_add: list[dict] = field(default_factory=list)
    single_pages_existing: list[dict] = field(default_factory=list)
    single_pages_orphan: list[dict] = field(default_factory=list)

    external_site_warnings: list[dict] = field(default_factory=list)
    skipped_rows: list[tuple[int, dict, str]] = field(default_factory=list)

    classification_counts: dict[str, int] = field(default_factory=dict)

    def has_changes(self) -> bool:
        return bool(
            self.google_sites_add
            or self.intranet_add
            or self.single_pages_add
        )


def compute_diff(rows: list[dict[str, str]], cfg: AppConfig) -> SyncDiff:
    """Compare classified sheet rows against current config, produce a diff.

    Matching strategy:
      - google_sites: collection slug derived from URL path
      - intranet: article_id (int)
      - single_pages: URL (exact)

    Technical fields (max_depth, url_filter, etc.) are never touched.
    Orphans (in config but not in sheet) are reported but not deleted.
    """
    diff = SyncDiff()
    counts: dict[str, int] = {}

    # Index current config. For google_sites we match on URL path prefix
    # (e.g. /falkenberg.se/unikumguider/) since the collection slug in config
    # may diverge from the slug derivable from the URL — a single canonical
    # source (the URL) avoids creating duplicates.
    existing_site_paths: set[str] = set()
    matched_collection_names: set[str] = set()
    for c in cfg.collections:
        for s in c.sites:
            path_key = _google_sites_path_key(s.url)
            if path_key:
                existing_site_paths.add(path_key)

    existing_article_ids = {a.article_id for a in cfg.intranet_articles}
    existing_single_urls = {p.url for p in cfg.single_pages}

    # Track what the sheet actually has, to detect orphans
    sheet_site_paths: set[str] = set()
    sheet_article_ids: set[int] = set()
    sheet_single_urls: set[str] = set()

    for idx, row in enumerate(rows, start=2):  # row 1 is header
        kind, attrs = classify_row(row)
        counts[kind] = counts.get(kind, 0) + 1

        if kind == "google_sites":
            path_key = _google_sites_path_key(attrs["url"])
            sheet_site_paths.add(path_key)
            target = {**attrs}
            if path_key in existing_site_paths:
                diff.google_sites_existing.append(target)
                # Record which existing collection this sheet row matches,
                # so we don't flag it as an orphan below.
                for c in cfg.collections:
                    if any(_google_sites_path_key(s.url) == path_key for s in c.sites):
                        matched_collection_names.add(c.name)
            else:
                diff.google_sites_add.append(target)

        elif kind == "intranet":
            article_id = attrs["article_id"]
            sheet_article_ids.add(article_id)
            if article_id in existing_article_ids:
                diff.intranet_existing.append(attrs)
            else:
                diff.intranet_add.append(attrs)

        elif kind == "single_page":
            url = attrs["url"]
            sheet_single_urls.add(url)
            if url in existing_single_urls:
                diff.single_pages_existing.append(attrs)
            else:
                diff.single_pages_add.append(attrs)

        elif kind == "external_site":
            diff.external_site_warnings.append(attrs)

        elif kind == "skip":
            diff.skipped_rows.append((idx, row, attrs.get("reason", "")))

    # Orphans: in config but not represented in sheet
    for c in cfg.collections:
        if c.name not in matched_collection_names:
            diff.google_sites_orphan.append(c.name)
    for a in cfg.intranet_articles:
        if a.article_id not in sheet_article_ids:
            diff.intranet_orphan.append({
                "article_id": a.article_id,
                "collection": a.collection,
                "display_name": a.display_name,
            })
    for p in cfg.single_pages:
        if p.url not in sheet_single_urls:
            diff.single_pages_orphan.append({
                "url": p.url,
                "collection": p.collection,
                "display_name": p.display_name,
            })

    diff.classification_counts = counts
    return diff


# ---------------------------------------------------------------------------
# Render
# ---------------------------------------------------------------------------


def render_diff(diff: SyncDiff) -> str:
    """Human-readable diff for CLI output."""
    out: list[str] = []
    counts = diff.classification_counts

    out.append("Klassificering:")
    out.append(f"  Google Sites:  {counts.get('google_sites', 0)}")
    out.append(f"  Intranätsida:  {counts.get('intranet', 0)}")
    out.append(f"  Single pages:  {counts.get('single_page', 0)}")
    out.append(f"  External site: {counts.get('external_site', 0)} (varning, se nedan)")
    out.append(f"  Skip:          {counts.get('skip', 0)}")
    out.append("")
    out.append("Diff mot config.yaml:")
    out.append("")

    def _add(prefix: str, text: str) -> None:
        out.append(f"  {prefix} {text}")

    if not diff.has_changes() and not diff.google_sites_orphan \
            and not diff.intranet_orphan and not diff.single_pages_orphan \
            and not diff.external_site_warnings:
        out.append("  (inga ändringar)")

    for item in diff.google_sites_add:
        _add(click.style("[+]", fg="green"),
             f"collections.{item['collection']}  ← {item['url']}  \"{item['display_name']}\"")

    for item in diff.intranet_add:
        _add(click.style("[+]", fg="green"),
             f"intranet_articles[{item['article_id']}] collection='{item['collection']}'  \"{item['display_name']}\"")

    for item in diff.single_pages_add:
        _add(click.style("[+]", fg="green"),
             f"single_pages.{item['collection']}  ← {item['url']}  \"{item['display_name']}\"")

    for slug in diff.google_sites_orphan:
        _add(click.style("[!]", fg="yellow"),
             f"collections.{slug} saknas i ark — KVARSTÅR (inget tas bort)")

    for item in diff.intranet_orphan:
        _add(click.style("[!]", fg="yellow"),
             f"intranet_articles[{item['article_id']}] saknas i ark — KVARSTÅR")

    for item in diff.single_pages_orphan:
        _add(click.style("[!]", fg="yellow"),
             f"single_pages {item['url']} saknas i ark — KVARSTÅR")

    for item in diff.external_site_warnings:
        _add(click.style("[?]", fg="yellow"),
             f"'{item['display_name']}' {item['url']} kräver manuell external_sites-konfig (sitemap) — hoppas över")

    for row_num, row, reason in diff.skipped_rows:
        display = (row.get("Display name") or "").strip() or "(tomt)"
        _add(click.style("[-]", fg="red"),
             f"rad {row_num} \"{display}\" hoppas över: {reason}")

    return "\n".join(out)


# ---------------------------------------------------------------------------
# Apply (write back to config.yaml with ruamel round-trip)
# ---------------------------------------------------------------------------


def apply_diff(diff: SyncDiff, config_path: Path) -> None:
    """Apply additions from diff to config.yaml, preserving comments/order.

    Writes atomically via temp-file + os.replace(). Orphans are NOT removed.
    Existing entries are NOT modified.
    """
    yaml_rt = YAML(typ="rt")
    yaml_rt.preserve_quotes = True
    yaml_rt.indent(mapping=2, sequence=4, offset=2)
    yaml_rt.width = 4096  # prevent wrapping long URLs across lines

    with open(config_path) as f:
        data = yaml_rt.load(f)

    if data is None:
        data = CommentedMap()

    # --- collections (Google Sites) ---
    if diff.google_sites_add:
        collections = data.setdefault("collections", CommentedSeq())
        for item in diff.google_sites_add:
            new_site = CommentedMap()
            new_site["url"] = item["url"]
            new_site["max_depth"] = 3
            ad = CommentedSeq(["sites.google.com"])
            new_site["allowed_domains"] = ad
            new_site["url_filter"] = _derive_url_filter(item["url"])

            new_collection = CommentedMap()
            new_collection["name"] = item["collection"]
            sites_seq = CommentedSeq()
            sites_seq.append(new_site)
            new_collection["sites"] = sites_seq
            collections.append(new_collection)

    # --- intranet_articles ---
    if diff.intranet_add:
        articles = data.setdefault("intranet_articles", CommentedSeq())
        for item in diff.intranet_add:
            entry = CommentedMap()
            entry["collection"] = item["collection"]
            entry["article_id"] = item["article_id"]
            entry["display_name"] = item["display_name"]
            if item.get("follow_links"):
                entry["follow_links"] = True
            if item.get("follow_external_links"):
                entry["follow_external_links"] = True
            articles.append(entry)

    # --- single_pages ---
    if diff.single_pages_add:
        pages = data.setdefault("single_pages", CommentedSeq())
        for item in diff.single_pages_add:
            entry = CommentedMap()
            entry["collection"] = item["collection"]
            entry["url"] = item["url"]
            entry["display_name"] = item["display_name"]
            pages.append(entry)

    # Atomic write: tmp file in same directory, then rename
    tmp_fd, tmp_path = tempfile.mkstemp(
        prefix=".config.yaml.", suffix=".tmp", dir=str(config_path.parent)
    )
    try:
        with os.fdopen(tmp_fd, "w") as f:
            yaml_rt.dump(data, f)
        os.replace(tmp_path, config_path)
    except Exception:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise


def _derive_url_filter(url: str) -> str:
    """Given a Google Sites URL, derive a url_filter string.

    https://sites.google.com/falkenberg.se/fokus-ai → '/falkenberg.se/fokus-ai/'
    """
    parsed = urlparse(url)
    parts = [p for p in parsed.path.split("/") if p]
    if len(parts) >= 2:
        return f"/{parts[0]}/{parts[1]}/"
    return parsed.path
