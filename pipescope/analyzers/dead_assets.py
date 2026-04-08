"""Dead / unused asset detection."""

from __future__ import annotations

from pipescope.models import Finding


def analyze() -> list[Finding]:
    """Return findings for assets with no downstream consumers (stub)."""
    return []
