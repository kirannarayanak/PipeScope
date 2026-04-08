"""Tests for core Pydantic models."""

import json

import pytest
from pydantic import ValidationError

from pipescope.models import Asset, AssetType, Finding, ScanResult, Severity


def test_asset_serializes_to_json_and_validates() -> None:
    asset = Asset(
        name="public.dim_customers",
        asset_type=AssetType.TABLE,
        file_path="models/dim_customers.sql",
        columns=["id", "email"],
        has_tests=True,
        tags={"layer": "marts"},
    )
    raw = asset.model_dump_json()
    data = json.loads(raw)
    assert data["name"] == "public.dim_customers"
    assert data["asset_type"] == "table"
    assert data["columns"] == ["id", "email"]
    assert data["tags"] == {"layer": "marts"}

    restored = Asset.model_validate_json(raw)
    assert restored == asset


def test_asset_rejects_invalid_asset_type() -> None:
    with pytest.raises(ValidationError):
        Asset.model_validate(
            {
                "name": "x",
                "asset_type": "not_a_real_type",
                "file_path": "a.sql",
            }
        )


def test_scan_result_empty_defaults() -> None:
    r = ScanResult()
    assert r.assets == []
    assert r.edges == []
    assert r.findings == []
    assert r.scores == {}


def test_finding_uses_severity_enum() -> None:
    f = Finding(
        severity=Severity.WARNING,
        category="dead_asset",
        asset_name="legacy.orders",
        message="No downstream references.",
        file_path="sql/legacy.sql",
    )
    assert f.model_dump()["severity"] == "warning"
