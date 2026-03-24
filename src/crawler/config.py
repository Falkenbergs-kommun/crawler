"""Load YAML config and environment variables."""

import os
from dataclasses import dataclass, field
from pathlib import Path

import yaml
from dotenv import load_dotenv


@dataclass
class SiteConfig:
    url: str
    max_depth: int = 3
    allowed_domains: list[str] = field(default_factory=list)
    url_filter: str = ""


@dataclass
class CollectionConfig:
    name: str
    sites: list[SiteConfig] = field(default_factory=list)


@dataclass
class ExternalSiteConfig:
    name: str  # Qdrant collection name
    base_url: str
    discovery: str = "sitemap"  # "sitemap" | "crawl"
    sitemaps: list[str] = field(default_factory=list)
    start_url: str = ""  # For discovery: crawl
    max_depth: int = 3  # For discovery: crawl
    document_extensions: list[str] = field(
        default_factory=lambda: [".pdf", ".docx", ".pptx"]
    )
    skip_extensions: list[str] = field(default_factory=list)
    exclude_patterns: list[str] = field(default_factory=list)
    max_concurrent: int = 5
    delay_between_requests: float = 0.5
    js_rendering: bool = False
    user_agent: str = "FalkenbergKommun-RAG-Bot/1.0"
    ocr: bool = True


@dataclass
class AppConfig:
    openai_api_key: str
    qdrant_url: str
    qdrant_api_key: str | None
    collections: list[CollectionConfig]
    external_sites: list[ExternalSiteConfig] = field(default_factory=list)


def load_config(config_path: str = "config.yaml") -> AppConfig:
    """Load config from YAML file and .env."""
    env_path = Path(config_path).parent / ".env"
    load_dotenv(env_path)

    openai_api_key = os.environ.get("OPENAI_API_KEY", "")
    qdrant_url = os.environ.get("QDRANT_URL", "http://localhost:6333")
    qdrant_api_key = os.environ.get("QDRANT_API_KEY") or None

    if not openai_api_key:
        raise ValueError("OPENAI_API_KEY must be set in .env")

    with open(config_path) as f:
        raw = yaml.safe_load(f)

    collections = []
    for coll in raw.get("collections", []):
        sites = [SiteConfig(**s) for s in coll.get("sites", [])]
        collections.append(CollectionConfig(name=coll["name"], sites=sites))

    external_sites = []
    for ext in raw.get("external_sites", []):
        external_sites.append(ExternalSiteConfig(
            name=ext["name"],
            base_url=ext.get("base_url", ""),
            discovery=ext.get("discovery", "sitemap"),
            sitemaps=ext.get("sitemaps", []),
            start_url=ext.get("start_url", ""),
            max_depth=ext.get("max_depth", 3),
            document_extensions=ext.get("document_extensions", [".pdf", ".docx", ".pptx"]),
            skip_extensions=ext.get("skip_extensions", []),
            exclude_patterns=ext.get("exclude_patterns", []),
            max_concurrent=ext.get("max_concurrent", 5),
            delay_between_requests=ext.get("delay_between_requests", 0.5),
            js_rendering=ext.get("js_rendering", False),
            user_agent=ext.get("user_agent", "FalkenbergKommun-RAG-Bot/1.0"),
            ocr=ext.get("ocr", True),
        ))

    return AppConfig(
        openai_api_key=openai_api_key,
        qdrant_url=qdrant_url,
        qdrant_api_key=qdrant_api_key,
        collections=collections,
        external_sites=external_sites,
    )
