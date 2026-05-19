#!/usr/bin/env python3
"""Compatibility wrapper for the packaged parquet finalizer."""

from __future__ import annotations

import sys

from KISTI_DB_Manager.parquet_finalize import main


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
