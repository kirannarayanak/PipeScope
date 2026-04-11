"""Documentation coverage from ``Asset.has_docs`` (dbt schema, SQL comments, Airflow DAG)."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from lineagescope.models import Asset, AssetType, Finding, Severity


def _doc_scope(assets: list[Asset]) -> list[Asset]:
    """Exclude synthetic query-block assets from SQL files."""
    return [a for a in assets if a.tags.get("role") != "query_block"]


def _severity_missing_doc(asset: Asset) -> Severity:
    if asset.asset_type in (AssetType.DBT_MODEL, AssetType.AIRFLOW_DAG):
        return Severity.WARNING
    return Severity.INFO


@dataclass
class DocumentationCoverageResult:
    """Aggregated documentation coverage score, counts, and findings."""

    findings: list[Finding] = field(default_factory=list)
    score: int = 100
    documented_count: int = 0
    total_count: int = 0
    coverage_ratio: float | None = None

    def to_analytics_dict(self) -> dict[str, Any]:
        return {
            "documentation_score": self.score,
            "documented_count": self.documented_count,
            "total_count": self.total_count,
            "coverage_ratio": self.coverage_ratio,
        }


def analyze_documentation_coverage(assets: list[Asset]) -> DocumentationCoverageResult:
    """Score ``(documented_count / total_count) * 100`` and flag missing documentation.

    ``has_docs`` is set by parsers: dbt ``schema.yml`` descriptions, SQL leading comments,
    Airflow ``doc_md`` / ``doc`` on ``DAG()``, Spark module docstrings.
    """
    scope = _doc_scope(assets)
    if not scope:
        return DocumentationCoverageResult(
            score=100,
            documented_count=0,
            total_count=0,
            coverage_ratio=None,
        )

    documented_count = sum(1 for a in scope if a.has_docs)
    total_count = len(scope)
    ratio = documented_count / total_count if total_count else None
    score = (
        max(0, min(100, int(round(ratio * 100))))
        if ratio is not None
        else 100
    )

    findings: list[Finding] = []
    for asset in scope:
        if asset.has_docs:
            continue
        findings.append(
            Finding(
                severity=_severity_missing_doc(asset),
                category="missing_documentation",
                asset_name=asset.name,
                message=(
                    "No documentation: add schema description, SQL header comment, or DAG doc_md"
                ),
                file_path=asset.file_path or None,
            )
        )

    return DocumentationCoverageResult(
        findings=findings,
        score=score,
        documented_count=documented_count,
        total_count=total_count,
        coverage_ratio=round(ratio, 6) if ratio is not None else None,
    )


def analyze() -> list[Finding]:
    """Stub without assets; use :func:`analyze_documentation_coverage`."""
    return []
