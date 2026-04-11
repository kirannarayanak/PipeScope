"""Asset-specific parsers (SQL, dbt, Airflow, Spark) and unified routing."""

from __future__ import annotations

from pathlib import Path

from lineagescope.models import Asset, Edge
from lineagescope.scanner import DiscoveredFile

from .airflow_parser import parse_airflow_file as parse_airflow_dag
from .dbt_parser import parse_dbt_model, parse_dbt_project, parse_dbt_schema
from .odcs_parser import ParsedContract, parse_odcs_file
from .spark_parser import parse_spark_file as parse_spark_job
from .sql_parser import parse_sql_file


def _display_path(path: Path, scan_root: Path | None) -> str:
    if scan_root is None:
        return str(path)
    try:
        return str(path.resolve().relative_to(scan_root.resolve()))
    except ValueError:
        return str(path)


def parse_file(
    f: DiscoveredFile,
    dialect: str | None = None,
    *,
    scan_root: Path | None = None,
) -> tuple[list[Asset], list[Edge]]:
    """Parse *f* by ``file_type`` (SQL, dbt, Airflow, Spark) and return assets + edges.

    *dialect* is passed to SQLGlot for ``sql`` only. *scan_root* makes ``file_path``
    in assets relative for stable reports. Caller should handle I/O and parser
    exceptions if strict behavior is required.
    """
    content = f.path.read_text(encoding="utf-8", errors="ignore")
    display = _display_path(f.path, scan_root)
    t = f.file_type
    if t == "sql":
        return parse_sql_file(display, content, dialect)
    if t == "dbt_model":
        return parse_dbt_model(display, content)
    if t == "dbt_schema":
        return parse_dbt_schema(display, content)
    if t == "airflow_dag":
        return parse_airflow_dag(display, content)
    if t == "spark_job":
        return parse_spark_job(display, content)
    return [], []


__all__ = [
    "ParsedContract",
    "parse_airflow_dag",
    "parse_dbt_model",
    "parse_dbt_project",
    "parse_dbt_schema",
    "parse_file",
    "parse_odcs_file",
    "parse_spark_job",
    "parse_sql_file",
]
