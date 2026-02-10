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
class AppConfig:
    openai_api_key: str
    qdrant_url: str
    qdrant_api_key: str | None
    collections: list[CollectionConfig]


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

    return AppConfig(
        openai_api_key=openai_api_key,
        qdrant_url=qdrant_url,
        qdrant_api_key=qdrant_api_key,
        collections=collections,
    )
