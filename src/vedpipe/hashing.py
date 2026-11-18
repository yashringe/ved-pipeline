"""Content hashing for idempotent ingestion.

A partner that retries a request sets a new ``sent_at`` but sends the same trip. The
hash ignores ``sent_at`` so the curation job can recognise such resends as exact
duplicates, while any change to the trip itself produces a new version.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from typing import Any

EXCLUDED_KEYS: frozenset[str] = frozenset({"sent_at"})


def canonical_json(payload: Mapping[str, Any]) -> str:
    """Key-sorted, whitespace-free JSON; NaN/inf are refused so the hash is well defined."""
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
                      allow_nan=False)


def content_sha256(payload: Mapping[str, Any]) -> str:
    """SHA-256 hex digest of the canonical JSON of ``payload`` without ``sent_at``."""
    body = {k: v for k, v in payload.items() if k not in EXCLUDED_KEYS}
    return hashlib.sha256(canonical_json(body).encode("utf-8")).hexdigest()
