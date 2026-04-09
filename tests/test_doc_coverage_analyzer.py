"""Documentation coverage analyzer tests."""

from __future__ import annotations

from pipescope.analyzers.doc_coverage import analyze_documentation_coverage
from pipescope.models import Asset, AssetType, Severity


def test_documentation_score_and_findings() -> None:
    assets = [
        Asset(name="a", asset_type=AssetType.TABLE, file_path="a.sql", has_docs=True),
        Asset(name="b", asset_type=AssetType.TABLE, file_path="b.sql", has_docs=False),
    ]
    r = analyze_documentation_coverage(assets)
    assert r.total_count == 2
    assert r.documented_count == 1
    assert r.score == 50
    assert len(r.findings) == 1
    assert r.findings[0].asset_name == "b"
    assert r.findings[0].severity == Severity.INFO


def test_query_blocks_excluded_from_denominator() -> None:
    assets = [
        Asset(
            name="q",
            asset_type=AssetType.VIEW,
            file_path="f.sql",
            has_docs=False,
            tags={"role": "query_block"},
        ),
        Asset(name="t", asset_type=AssetType.TABLE, file_path="f.sql", has_docs=True),
    ]
    r = analyze_documentation_coverage(assets)
    assert r.total_count == 1
    assert r.score == 100
    assert r.findings == []
