"""Extract text content from Google Drive/Docs links found in crawled pages."""

from __future__ import annotations

import io
import re
from dataclasses import dataclass

import click
import httpx
from pdfminer.high_level import extract_text as pdf_extract_text

# Patterns for extracting Google document IDs
_DRIVE_FILE_RE = re.compile(
    r"drive\.google\.com/file/d/([a-zA-Z0-9_-]+)"
)
_DOCS_RE = re.compile(
    r"docs\.google\.com/document/d/([a-zA-Z0-9_-]+)"
)
_SHEETS_RE = re.compile(
    r"docs\.google\.com/spreadsheets/d/([a-zA-Z0-9_-]+)"
)
_SLIDES_RE = re.compile(
    r"docs\.google\.com/presentation/d/([a-zA-Z0-9_-]+)"
)
_YOUTUBE_RE = re.compile(
    r"(?:youtube\.com/watch\?v=|youtube\.com/embed/|youtu\.be/)([a-zA-Z0-9_-]{11})"
)


@dataclass
class ExtractedDocument:
    source_url: str
    title: str
    text: str
    content_type: str  # "google_doc", "google_sheet", "drive_pdf", "youtube_video"


def find_google_links(urls: list[str]) -> list[tuple[str, str, str]]:
    """Find Google Drive/Docs/Sheets/Slides URLs and return (url, doc_id, doc_type)."""
    found = []
    seen_ids: set[str] = set()

    for url in urls:
        for regex, doc_type in [
            (_DOCS_RE, "google_doc"),
            (_SHEETS_RE, "google_sheet"),
            (_SLIDES_RE, "google_slides"),
            (_DRIVE_FILE_RE, "drive_file"),
        ]:
            match = regex.search(url)
            if match:
                doc_id = match.group(1)
                if doc_id not in seen_ids:
                    seen_ids.add(doc_id)
                    found.append((url, doc_id, doc_type))
                break

    return found


def _canonical_url(doc_id: str, doc_type: str) -> str:
    """Return a canonical URL for a Google document, stripping query/fragment variants."""
    if doc_type == "google_doc":
        return f"https://docs.google.com/document/d/{doc_id}"
    elif doc_type == "google_sheet":
        return f"https://docs.google.com/spreadsheets/d/{doc_id}"
    elif doc_type == "google_slides":
        return f"https://docs.google.com/presentation/d/{doc_id}"
    else:  # drive_file
        return f"https://drive.google.com/file/d/{doc_id}"


def find_youtube_ids(urls: list[str]) -> list[tuple[str, str]]:
    """Find YouTube video URLs and return (url, video_id)."""
    found = []
    seen_ids: set[str] = set()

    for url in urls:
        match = _YOUTUBE_RE.search(url)
        if match:
            vid_id = match.group(1)
            if vid_id not in seen_ids:
                seen_ids.add(vid_id)
                found.append((url, vid_id))

    return found


def _download_google_doc(doc_id: str) -> str:
    """Download a Google Doc as plain text."""
    url = f"https://docs.google.com/document/d/{doc_id}/export?format=txt"
    resp = httpx.get(url, follow_redirects=True, timeout=30)
    resp.raise_for_status()
    return resp.text


def _download_google_sheet(doc_id: str) -> str:
    """Download a Google Sheet as CSV text."""
    url = f"https://docs.google.com/spreadsheets/d/{doc_id}/export?format=csv"
    resp = httpx.get(url, follow_redirects=True, timeout=30)
    resp.raise_for_status()
    return resp.text


def _download_google_slides(doc_id: str) -> str:
    """Download Google Slides as plain text.

    Tries txt export first; falls back to scraping the public /pub HTML,
    since Slides export often returns 401 even for published presentations.
    """
    # Try direct export first (works for fully public presentations)
    export_url = f"https://docs.google.com/presentation/d/{doc_id}/export?format=txt"
    resp = httpx.get(export_url, follow_redirects=True, timeout=30)
    if resp.status_code == 200 and resp.text.strip():
        return resp.text

    # Fallback: scrape the published HTML version
    pub_url = f"https://docs.google.com/presentation/d/{doc_id}/pub"
    resp = httpx.get(pub_url, follow_redirects=True, timeout=30)
    resp.raise_for_status()

    # Extract visible text from slide frames (strip HTML tags)
    import re as _re
    # Remove script/style blocks, then tags, collapse whitespace
    html = _re.sub(r"<(script|style)[^>]*>.*?</\1>", "", resp.text, flags=_re.DOTALL)
    text = _re.sub(r"<[^>]+>", " ", html)
    text = _re.sub(r"\s+", " ", text).strip()
    return text


def _download_drive_file(doc_id: str) -> str:
    """Download a Drive file and extract text. Handles PDFs and text files."""
    url = f"https://drive.google.com/uc?export=download&id={doc_id}"
    resp = httpx.get(url, follow_redirects=True, timeout=60)
    resp.raise_for_status()

    content_type = resp.headers.get("content-type", "")

    if "pdf" in content_type:
        return pdf_extract_text(io.BytesIO(resp.content))
    elif "text" in content_type or "csv" in content_type:
        return resp.text
    else:
        # Try PDF extraction as fallback (many Drive files are PDFs)
        try:
            return pdf_extract_text(io.BytesIO(resp.content))
        except Exception:
            return ""


def _fetch_youtube_metadata(video_id: str) -> tuple[str, str]:
    """Fetch YouTube video title and author via oEmbed. Returns (title, author)."""
    url = f"https://noembed.com/embed?url=https://www.youtube.com/watch?v={video_id}"
    try:
        resp = httpx.get(url, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        return data.get("title", ""), data.get("author_name", "")
    except Exception:
        return "", ""


def _find_urls_in_html(html: str) -> list[str]:
    """Extract all Google Drive/Docs/YouTube URLs from raw HTML."""
    patterns = [
        r'(https?://drive\.google\.com/file/d/[a-zA-Z0-9_-]+[^"\'<> ]*)',
        r'(https?://docs\.google\.com/document/d/[a-zA-Z0-9_-]+[^"\'<> ]*)',
        r'(https?://docs\.google\.com/spreadsheets/d/[a-zA-Z0-9_-]+[^"\'<> ]*)',
        r'(https?://docs\.google\.com/presentation/d/[a-zA-Z0-9_-]+[^"\'<> ]*)',
    ]
    urls = []
    for pattern in patterns:
        urls.extend(re.findall(pattern, html))
    return urls


def extract_google_documents(
    external_links: list[str], page_url: str, raw_html: str = ""
) -> list[ExtractedDocument]:
    """Extract text content from all Google Drive/Docs links.

    Searches both crawl4ai's extracted links and raw HTML,
    since Drive links are often missed by link extraction.
    """
    results = []

    # Combine links from crawl4ai and from raw HTML scanning
    all_links = list(external_links)
    if raw_html:
        all_links.extend(_find_urls_in_html(raw_html))

    google_links = find_google_links(all_links)
    for link_url, doc_id, doc_type in google_links:
        canonical = _canonical_url(doc_id, doc_type)
        click.echo(f"    Extracting [{doc_type}]: {canonical}...")
        try:
            if doc_type == "google_doc":
                text = _download_google_doc(doc_id)
                title = text.split("\n")[0][:100] if text else doc_id
            elif doc_type == "google_sheet":
                text = _download_google_sheet(doc_id)
                title = f"Spreadsheet {doc_id[:8]}"
            elif doc_type == "google_slides":
                text = _download_google_slides(doc_id)
                title = text.split("\n")[0][:100] if text else doc_id
            else:  # drive_file
                text = _download_drive_file(doc_id)
                title = f"Document {doc_id[:8]}"

            if text and text.strip():
                results.append(ExtractedDocument(
                    source_url=canonical,
                    title=title,
                    text=text.strip(),
                    content_type=doc_type,
                ))
                click.echo(f"      Extracted {len(text)} chars")
            else:
                click.echo(f"      No text content found")
        except Exception as e:
            click.echo(f"      Failed: {e}")

    return results


def extract_youtube_metadata(
    external_links: list[str], html_content: str = ""
) -> list[ExtractedDocument]:
    """Extract YouTube video titles and authors from linked/embedded videos."""
    results = []

    # Find from external links
    yt_from_links = find_youtube_ids(external_links)

    # Also find from iframe src attributes in the HTML
    if html_content:
        iframe_urls = re.findall(
            r'<iframe[^>]*src=["\']([^"\']*youtube[^"\']*)["\']', html_content, re.IGNORECASE
        )
        yt_from_iframes = find_youtube_ids(iframe_urls)
        # Merge, dedup by video ID
        seen = {vid_id for _, vid_id in yt_from_links}
        for url, vid_id in yt_from_iframes:
            if vid_id not in seen:
                seen.add(vid_id)
                yt_from_links.append((url, vid_id))

    for url, vid_id in yt_from_links:
        title, author = _fetch_youtube_metadata(vid_id)
        if title:
            text = f"YouTube-video: {title}"
            if author:
                text += f" (av {author})"
            results.append(ExtractedDocument(
                source_url=f"https://www.youtube.com/watch?v={vid_id}",
                title=title,
                text=text,
                content_type="youtube_video",
            ))

    if results:
        click.echo(f"    Found {len(results)} YouTube videos with metadata")

    return results
