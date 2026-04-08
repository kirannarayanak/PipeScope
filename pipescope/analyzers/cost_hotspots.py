"""Cost hotspot estimation from static SQL patterns."""

from __future__ import annotations

from pipescope.models import Finding


def analyze() -> list[Finding]:
    """Flag SELECT *, missing partitions, etc. (stub)."""
    return []
