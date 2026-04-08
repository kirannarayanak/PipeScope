"""Tests for analyzer stubs."""

from pipescope.analyzers import (
    complexity,
    contracts,
    cost_hotspots,
    dead_assets,
    doc_coverage,
    ownership,
    test_coverage,
)


def test_analyzers_return_lists() -> None:
    for mod in (
        dead_assets,
        test_coverage,
        doc_coverage,
        complexity,
        ownership,
        contracts,
        cost_hotspots,
    ):
        assert mod.analyze() == []
