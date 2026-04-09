"""Tests for ownership analyzer (CODEOWNERS, dbt meta, git fallback)."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from pipescope.analyzers import ownership as ownership_mod
from pipescope.analyzers.ownership import (
    OwnershipAnalysisResult,
    analyze_ownership,
    codeowners_pattern_matches,
    parse_codeowners_text,
)
from pipescope.models import Asset, AssetType


def test_codeowners_glob_basename_only_matches_any_depth() -> None:
    assert codeowners_pattern_matches("*.sql", "models/marts/foo.sql") is True
    assert codeowners_pattern_matches("*.sql", "foo.sql") is True
    assert codeowners_pattern_matches("*.py", "models/marts/foo.sql") is False


def test_codeowners_slash_pattern_one_level() -> None:
    assert codeowners_pattern_matches("models/*.sql", "models/stg.sql") is True
    assert codeowners_pattern_matches("models/*.sql", "models/nested/stg.sql") is False


def test_codeowners_double_star() -> None:
    assert codeowners_pattern_matches("models/**/*.sql", "models/marts/f.sql") is True
    assert codeowners_pattern_matches("models/**", "models/a/b/c") is True


def test_parse_codeowners_skips_comments_and_blank() -> None:
    text = """
# team
*.sql @data-team
"""
    out = parse_codeowners_text(text)
    assert out == [("*.sql", ["@data-team"])]


def test_last_codeowners_pattern_wins() -> None:
    entries = parse_codeowners_text(
        "*.sql @first\n"
        "models/*.sql @second\n"
    )
    from pipescope.analyzers.ownership import _codeowners_owner_for_path

    assert _codeowners_owner_for_path(entries, "models/x.sql") == "@second"


def test_no_owner_without_git_meta_or_codeowners(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(ownership_mod, "_git_toplevel", lambda p: None)
    (tmp_path / "orphan.sql").write_text("select 1")
    asset = Asset(
        name="orphan",
        asset_type=AssetType.TABLE,
        file_path="orphan.sql",
    )
    r = analyze_ownership([asset], tmp_path)
    assert isinstance(r, OwnershipAnalysisResult)
    assert r.total_count == 1
    assert r.no_owner_count == 1
    assert r.assets_with_owner == 0
    assert any(f.category == "no_owner" for f in r.findings)


def test_codeowners_wins_over_dbt_meta(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(ownership_mod, "_git_toplevel", lambda p: None)
    (tmp_path / ".github").mkdir()
    (tmp_path / ".github" / "CODEOWNERS").write_text("*.sql @from-codeowners\n")
    (tmp_path / "m.sql").write_text("select 1")
    asset = Asset(
        name="m",
        asset_type=AssetType.DBT_MODEL,
        file_path="m.sql",
        owner="from-dbt-meta",
    )
    r = analyze_ownership([asset], tmp_path)
    assert r.no_owner_count == 0
    assert r.assets_with_owner == 1


def test_stale_finding_old_commit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(ownership_mod, "_git_toplevel", lambda p: tmp_path.resolve())
    (tmp_path / "old.sql").write_text("select 1")
    old = datetime.now(tz=UTC) - timedelta(days=400)

    def fake_git(_repo: Path, _rel: str) -> tuple[str | None, datetime | None]:
        return ("alice", old)

    monkeypatch.setattr(ownership_mod, "_git_last_commit_info", fake_git)

    asset = Asset(name="old", asset_type=AssetType.TABLE, file_path="old.sql")
    r = analyze_ownership([asset], tmp_path)
    assert any(f.category == "stale_asset" for f in r.findings)
    assert r.stale_count >= 1


def test_query_blocks_excluded_from_ownership_denominator(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(ownership_mod, "_git_toplevel", lambda p: None)
    (tmp_path / "y.sql").write_text("x")
    assets = [
        Asset(
            name="q",
            asset_type=AssetType.TABLE,
            file_path="x.sql",
            tags={"role": "query_block"},
        ),
        Asset(name="t", asset_type=AssetType.TABLE, file_path="y.sql"),
    ]
    r = analyze_ownership(assets, tmp_path)
    assert r.total_count == 1
