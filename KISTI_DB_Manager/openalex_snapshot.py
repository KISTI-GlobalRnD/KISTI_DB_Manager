from __future__ import annotations

import json
import urllib.request
from dataclasses import dataclass
from datetime import date, timedelta


MANIFEST_URL = "https://openalex.s3.amazonaws.com/data/{entity}/manifest"
PUBLIC_HTTP_PREFIX = "https://openalex.s3.amazonaws.com/"
PUBLIC_S3_PREFIX = "s3://openalex/"

FULL_SERVING_CORE_ENTITIES: list[str] = [
    "works",
    "authors",
    "institutions",
    "sources",
    "topics",
    "concepts",
    "publishers",
    "funders",
]

FULL_SERVING_REFERENCE_ENTITIES: list[str] = [
    "awards",
    "keywords",
    "fields",
    "subfields",
    "domains",
    "sdgs",
    "institution-types",
    "source-types",
    "work-types",
    "languages",
    "countries",
    "continents",
    "licenses",
]

FULL_SERVING_ENTITIES: list[str] = FULL_SERVING_CORE_ENTITIES + FULL_SERVING_REFERENCE_ENTITIES


@dataclass(frozen=True)
class ManifestEntry:
    entity: str
    updated_date: str
    url_s3: str
    url_http: str
    content_length: int
    rel_path: str


def validate_date(value: str) -> str:
    date.fromisoformat(value)
    return value


def next_date(value: str) -> str:
    return (date.fromisoformat(validate_date(value)) + timedelta(days=1)).isoformat()


def fetch_manifest(entity: str) -> list[ManifestEntry]:
    with urllib.request.urlopen(MANIFEST_URL.format(entity=entity), timeout=120) as response:
        payload = json.load(response)

    entries: list[ManifestEntry] = []
    for raw in payload.get("entries", []):
        url_s3 = str(raw["url"])
        if not url_s3.startswith(PUBLIC_S3_PREFIX):
            continue
        rel_path = url_s3[len(PUBLIC_S3_PREFIX) :]
        updated_date = rel_path.split("updated_date=", 1)[1].split("/", 1)[0]
        entries.append(
            ManifestEntry(
                entity=entity,
                updated_date=updated_date,
                url_s3=url_s3,
                url_http=PUBLIC_HTTP_PREFIX + rel_path,
                content_length=int(raw.get("meta", {}).get("content_length", 0) or 0),
                rel_path=rel_path,
            )
        )
    return entries


def filter_entries(
    entries: list[ManifestEntry],
    *,
    start_date: str,
    end_date: str,
    entity: str | None = None,
) -> list[ManifestEntry]:
    validate_date(start_date)
    validate_date(end_date)
    selected: list[ManifestEntry] = []
    for entry in entries:
        if entity is not None and entry.entity != entity:
            continue
        if not entry.rel_path.startswith(f"data/{entry.entity}/updated_date="):
            continue
        if start_date <= entry.updated_date <= end_date:
            selected.append(entry)
    selected.sort(key=lambda x: (x.updated_date, x.rel_path))
    return selected


def latest_date(entries: list[ManifestEntry]) -> str | None:
    if not entries:
        return None
    return max(entry.updated_date for entry in entries)


def total_bytes(entries: list[ManifestEntry]) -> int:
    return sum(max(0, int(entry.content_length)) for entry in entries)
