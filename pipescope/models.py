"""Core data models for assets, graph edges, and findings."""

from __future__ import annotations

from enum import StrEnum
from typing import Any

from pydantic import BaseModel, Field


class Severity(StrEnum):
    """Finding severity for reports and CI."""

    CRITICAL = "critical"
    WARNING = "warning"
    INFO = "info"


class Asset(BaseModel):
    """A discovered data asset (table, model, task, file)."""

    id: str
    kind: str = Field(description="e.g. sql_file, dbt_model, airflow_dag")
    path: str
    name: str | None = None
    extra: dict[str, Any] = Field(default_factory=dict)


class Edge(BaseModel):
    """Directed dependency between two assets."""

    source_id: str
    target_id: str
    kind: str = Field(default="data_flow", description="edge semantic label")
    extra: dict[str, Any] = Field(default_factory=dict)


class Finding(BaseModel):
    """A single issue or insight from an analyzer."""

    code: str
    message: str
    severity: Severity = Severity.INFO
    asset_id: str | None = None
    extra: dict[str, Any] = Field(default_factory=dict)
