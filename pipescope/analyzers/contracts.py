"""Data contract compliance (ODCS vs declared columns and types)."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any

from pipescope.graph import PipelineGraph
from pipescope.models import Asset, Finding, Severity
from pipescope.parsers.odcs_parser import ParsedContract


def _scope_assets(assets: list[Asset]) -> list[Asset]:
    return [a for a in assets if a.tags.get("role") != "query_block"]


def _canonical_type(raw: str | None) -> str | None:
    """Normalize physical/logical types for fuzzy equality."""
    if not raw:
        return None
    s = raw.lower().strip()
    s = re.sub(r"\s+", " ", s)
    base = re.split(r"[\s(]", s, maxsplit=1)[0]
    synonyms: dict[str, str] = {
        "int": "integer",
        "integer": "integer",
        "bigint": "integer",
        "smallint": "integer",
        "tinyint": "integer",
        "varchar": "string",
        "nvarchar": "string",
        "text": "string",
        "string": "string",
        "float": "float",
        "double": "float",
        "real": "float",
        "decimal": "numeric",
        "numeric": "numeric",
        "number": "numeric",
        "bool": "boolean",
        "boolean": "boolean",
        "timestamp": "timestamp",
        "timestamptz": "timestamp",
        "datetime": "timestamp",
        "date": "date",
    }
    return synonyms.get(base, base)


def find_asset_for_contract(dataset_name: str, assets: list[Asset]) -> Asset | None:
    """Match contract dataset name to a graph asset (exact, case-insensitive, or table stem)."""
    scope = _scope_assets(assets)
    if not dataset_name.strip():
        return None
    dn = dataset_name.strip()
    for a in scope:
        if a.name == dn:
            return a
    dnl = dn.lower()
    for a in scope:
        if a.name.lower() == dnl:
            return a
    stem = dn.split(".")[-1]
    for a in scope:
        if a.name.split(".")[-1].lower() == stem.lower():
            return a
    return None


def _compare_columns(
    contract: ParsedContract,
    asset: Asset,
) -> list[Finding]:
    findings: list[Finding] = []
    ccols = set(contract.columns.keys())
    acols = set(asset.columns)
    fp = contract.file_path

    for col in sorted(ccols - acols):
        findings.append(
            Finding(
                severity=Severity.WARNING,
                category="contract_missing_column",
                asset_name=contract.dataset_name,
                message=f"Contract requires column '{col}' not present on asset '{asset.name}'",
                file_path=fp,
            )
        )
    for col in sorted(acols - ccols):
        findings.append(
            Finding(
                severity=Severity.INFO,
                category="contract_extra_column",
                asset_name=contract.dataset_name,
                message=f"Asset '{asset.name}' has column '{col}' not listed in contract",
                file_path=fp,
            )
        )

    for col in sorted(ccols & acols):
        expected = contract.columns.get(col)
        actual = asset.column_types.get(col)
        if expected and actual:
            ce = _canonical_type(expected)
            ca = _canonical_type(actual)
            if ce and ca and ce != ca:
                findings.append(
                    Finding(
                        severity=Severity.WARNING,
                        category="contract_type_mismatch",
                        asset_name=contract.dataset_name,
                        message=(
                            f"Column '{col}': contract type '{expected}' vs actual '{actual}' "
                            f"(asset '{asset.name}')"
                        ),
                        file_path=fp,
                    )
                )

    return findings


@dataclass
class ContractComplianceResult:
    findings: list[Finding] = field(default_factory=list)
    score: int = 100
    total_contracts: int = 0
    compliant_contracts: int = 0
    compliance_ratio: float | None = None

    def to_analytics_dict(self) -> dict[str, Any]:
        return {
            "contract_compliance_score": self.score,
            "total_contracts": self.total_contracts,
            "compliant_contracts": self.compliant_contracts,
            "compliance_ratio": self.compliance_ratio,
        }


def analyze_contract_compliance(
    _pg: PipelineGraph,
    assets: list[Asset],
    contracts: list[ParsedContract],
) -> ContractComplianceResult:
    """Compare ODCS column expectations to assets (CREATE / dbt ``schema.yml`` types)."""
    if not contracts:
        return ContractComplianceResult(
            score=100,
            total_contracts=0,
            compliant_contracts=0,
            compliance_ratio=None,
        )

    scoped = _scope_assets(assets)
    findings: list[Finding] = []
    compliant = 0
    total = 0

    for contract in contracts:
        if not contract.columns:
            continue
        total += 1
        asset = find_asset_for_contract(contract.dataset_name, scoped)
        if asset is None:
            findings.append(
                Finding(
                    severity=Severity.WARNING,
                    category="contract_asset_not_found",
                    asset_name=contract.dataset_name,
                    message=(
                        f"No matching asset for contract dataset '{contract.dataset_name}' "
                        f"(file {contract.file_path})"
                    ),
                    file_path=contract.file_path,
                )
            )
            continue
        issues = _compare_columns(contract, asset)
        findings.extend(issues)
        if not issues:
            compliant += 1

    ratio = compliant / total if total else None
    score = max(0, min(100, int(round(ratio * 100)))) if ratio is not None else 100

    return ContractComplianceResult(
        findings=findings,
        score=score,
        total_contracts=total,
        compliant_contracts=compliant,
        compliance_ratio=round(ratio, 6) if ratio is not None else None,
    )


def analyze() -> list[Finding]:
    """Stub without scan context; use :func:`analyze_contract_compliance`."""
    return []
