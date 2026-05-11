from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any


def normalize_openalex_work_id(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if not text:
        return ""
    return text.rsplit("/", 1)[-1]


def parse_abstract_inverted_index(value: Any) -> dict[str, list[int]] | None:
    if value is None:
        return None
    payload = value
    if isinstance(payload, str):
        text = payload.strip()
        if not text or text.lower() == "null":
            return None
        try:
            payload = json.loads(text)
        except Exception:
            return None
    if not isinstance(payload, Mapping):
        return None

    parsed: dict[str, list[int]] = {}
    for token, positions in payload.items():
        if not isinstance(positions, list):
            continue
        valid_positions = [int(pos) for pos in positions if isinstance(pos, int)]
        if valid_positions:
            parsed[str(token)] = valid_positions
    return parsed or None


def reconstruct_abstract_text(value: Any) -> dict[str, Any]:
    inv = parse_abstract_inverted_index(value)
    if not inv:
        return {
            "has_abstract": "N",
            "abstract": "",
            "token_count": 0,
            "position_count": 0,
            "unique_positions": 0,
            "collisions": 0,
        }

    pos_to_tokens: dict[int, list[str]] = {}
    token_count = 0
    position_count = 0
    collisions = 0
    for token, positions in inv.items():
        token_count += 1
        for pos in positions:
            if pos in pos_to_tokens:
                collisions += 1
            pos_to_tokens.setdefault(int(pos), []).append(str(token))
            position_count += 1

    ordered_positions = sorted(pos_to_tokens)
    words = [str(pos_to_tokens[pos][0]) for pos in ordered_positions if pos_to_tokens[pos]]
    abstract = " ".join(words).strip()
    return {
        "has_abstract": "Y" if abstract else "N",
        "abstract": abstract,
        "token_count": int(token_count),
        "position_count": int(position_count),
        "unique_positions": int(len(pos_to_tokens)),
        "collisions": int(collisions),
    }
