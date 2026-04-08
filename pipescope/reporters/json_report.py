"""JSON output for CI/CD."""

from __future__ import annotations

import json
from pathlib import Path


def write_json(path: Path, data: object) -> None:
    """Serialize *data* to JSON."""
    path.write_text(json.dumps(data, indent=2), encoding="utf-8")
