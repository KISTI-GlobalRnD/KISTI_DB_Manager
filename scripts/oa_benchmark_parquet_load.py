#!/usr/bin/env python3
"""Compatibility wrapper for the packaged OpenAlex parquet load benchmark."""

from __future__ import annotations

import sys

from KISTI_DB_Manager import openalex_benchmark as _impl

globals().update({name: getattr(_impl, name) for name in dir(_impl) if not name.startswith("__")})


if __name__ == "__main__":
    raise SystemExit(_impl.main(sys.argv[1:]))
