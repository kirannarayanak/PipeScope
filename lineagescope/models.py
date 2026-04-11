"""Core data models for assets, graph edges, scan results, and findings."""

from __future__ import annotations

from enum import StrEnum

from pydantic import BaseModel, Field


class AssetType(StrEnum):
    """Kind of data asset."""

    TABLE = "table"
    VIEW = "view"
    DBT_MODEL = "dbt_model"
    DBT_SOURCE = "dbt_source"
    AIRFLOW_DAG = "airflow_dag"
    AIRFLOW_TASK = "airflow_task"
    SPARK_JOB = "spark_job"


class Severity(StrEnum):
    """Finding severity for reports and CI."""

    CRITICAL = "critical"
    WARNING = "warning"
    INFO = "info"


class Asset(BaseModel):
    """A single data asset (table, model, DAG, etc.)."""

    name: str  # e.g. "public.dim_customers"
    asset_type: AssetType
    file_path: str
    columns: list[str] = Field(default_factory=list)
    column_types: dict[str, str] = Field(default_factory=dict)
    has_tests: bool = False
    has_docs: bool = False
    owner: str | None = None
    description: str | None = None
    tags: dict[str, str] = Field(default_factory=dict)


class Edge(BaseModel):
    """A dependency edge between two assets."""

    source: str  # upstream asset name
    target: str  # downstream asset name
    column_mapping: dict[str, list[str]] = Field(default_factory=dict)


class Finding(BaseModel):
    """A single issue found by an analyzer."""

    severity: Severity
    category: str  # e.g. "dead_asset", "missing_test"
    asset_name: str
    message: str
    file_path: str | None = None


class ScanResult(BaseModel):
    """Complete result of a LineageScope scan."""

    assets: list[Asset] = Field(default_factory=list)
    edges: list[Edge] = Field(default_factory=list)
    findings: list[Finding] = Field(default_factory=list)
    scores: dict[str, int] = Field(default_factory=dict)
