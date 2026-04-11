"""Test coverage scoring from ``has_tests`` and lineage risk."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from lineagescope.graph import PipelineGraph
from lineagescope.models import Asset, AssetType, Finding, Severity


def _coverage_scope(assets: list[Asset]) -> list[Asset]:
    """Prefer dbt models; otherwise tables/views (exclude sources, Airflow, Spark)."""
    dbt = [a for a in assets if a.asset_type == AssetType.DBT_MODEL]
    if dbt:
        return dbt
    return [
        a
        for a in assets
        if a.asset_type
        not in (
            AssetType.DBT_SOURCE,
            AssetType.AIRFLOW_DAG,
            AssetType.AIRFLOW_TASK,
            AssetType.SPARK_JOB,
        )
    ]


def _is_staging(asset: Asset) -> bool:
    fp = (asset.file_path or "").lower()
    name = asset.name.lower()
    return (
        "staging" in fp
        or "/stg_" in fp
        or "\\stg_" in fp
        or name.startswith("stg_")
    )


def _is_mart_or_final(asset: Asset) -> bool:
    fp = (asset.file_path or "").lower()
    name = asset.name.lower()
    return (
        "marts" in fp
        or "/mart/" in fp
        or "intermediate" in fp
        or "/int/" in fp
        or name.startswith(("fct_", "dim_", "mart_"))
    )


def _downstream_dependents(pg: PipelineGraph, name: str) -> int:
    return int(pg.g.out_degree(name))


def _severity_for_missing_tests(
    deps: int,
    staging: bool,
    *,
    critical_downstream_threshold: int,
) -> Severity:
    """CRITICAL / WARNING / INFO with staging treated as lower risk."""
    if deps == 0:
        return Severity.INFO
    if deps > critical_downstream_threshold:
        return Severity.WARNING if staging else Severity.CRITICAL
    if deps > 0:
        return Severity.INFO if staging else Severity.WARNING
    return Severity.INFO


@dataclass
class TestCoverageAnalysisResult:
    findings: list[Finding] = field(default_factory=list)
    score: int = 100
    tested_count: int = 0
    total_count: int = 0
    coverage_ratio: float | None = None
    critical_downstream_threshold: int = 10

    def to_analytics_dict(self) -> dict[str, Any]:
        return {
            "test_coverage_score": self.score,
            "tested_count": self.tested_count,
            "total_count": self.total_count,
            "coverage_ratio": self.coverage_ratio,
            "critical_downstream_threshold": self.critical_downstream_threshold,
        }


DEFAULT_CRITICAL_DOWNSTREAM_THRESHOLD = 10


def analyze_test_coverage(
    pg: PipelineGraph,
    assets: list[Asset],
    *,
    critical_downstream_threshold: int = DEFAULT_CRITICAL_DOWNSTREAM_THRESHOLD,
) -> TestCoverageAnalysisResult:
    """Score test coverage and emit findings for gaps and weak dbt tests.

    Score: ``(tested_count / total_count) * 100`` over :func:`_coverage_scope`.

    Findings (no tests, only if downstream dependents > 0):

    - CRITICAL — ``dependents > critical_downstream_threshold`` and not staging.
    - WARNING — ``dependents > 0`` and not staging (and not CRITICAL).
    - INFO — staging-only risk for ``0 < dependents <= threshold``.

    INFO — ``has_tests`` and ``tags.test_richness == low`` on mart/final models.
    """
    threshold = max(0, int(critical_downstream_threshold))
    scope = _coverage_scope(assets)
    if not scope:
        return TestCoverageAnalysisResult(
            score=100,
            tested_count=0,
            total_count=0,
            coverage_ratio=None,
            critical_downstream_threshold=threshold,
        )

    tested_count = sum(1 for a in scope if a.has_tests)
    total_count = len(scope)
    ratio = tested_count / total_count if total_count else None
    score = (
        max(0, min(100, int(round(ratio * 100))))
        if ratio is not None
        else 100
    )

    findings: list[Finding] = []

    for asset in scope:
        if asset.has_tests:
            continue
        if asset.name not in pg.g:
            continue
        deps = _downstream_dependents(pg, asset.name)
        if deps == 0:
            continue
        staging = _is_staging(asset)
        sev = _severity_for_missing_tests(
            deps,
            staging,
            critical_downstream_threshold=threshold,
        )
        findings.append(
            Finding(
                severity=sev,
                category="missing_test",
                asset_name=asset.name,
                message=(
                    f"No tests; {deps} downstream dependent(s); "
                    f"staging={staging!s} (lower risk when True)"
                ),
                file_path=asset.file_path or None,
            )
        )

    for asset in scope:
        if not asset.has_tests:
            continue
        if asset.tags.get("test_richness") != "low":
            continue
        if _is_staging(asset) or not _is_mart_or_final(asset):
            continue
        findings.append(
            Finding(
                severity=Severity.INFO,
                category="weak_test_coverage",
                asset_name=asset.name,
                message=(
                    "Tests present but schema suggests only not_null-style tests; "
                    "consider uniqueness or relationships on mart/final models"
                ),
                file_path=asset.file_path or None,
            )
        )

    return TestCoverageAnalysisResult(
        findings=findings,
        score=score,
        tested_count=tested_count,
        total_count=total_count,
        coverage_ratio=round(ratio, 6) if ratio is not None else None,
        critical_downstream_threshold=threshold,
    )


def analyze() -> list[Finding]:
    """Stub without graph; use :func:`analyze_test_coverage`."""
    return []
