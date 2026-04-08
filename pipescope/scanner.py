"""Repository walker: classify data-related files by type."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class DiscoveredFile:
    """A file on disk and its inferred role."""

    path: Path
    file_type: str  # sql, dbt_model, dbt_schema, dbt_project, airflow_dag, spark_job, data_contract


def scan_directory(root: Path) -> list[DiscoveredFile]:
    """Walk a repo and classify every data-related file."""
    root = root.resolve()
    if not root.is_dir():
        return []

    files: list[DiscoveredFile] = []

    for path in root.rglob("*"):
        if path.is_dir() or path.name.startswith("."):
            continue

        # dbt models: models/**/*.sql
        if _is_dbt_model(path, root):
            files.append(DiscoveredFile(path, "dbt_model"))
        # dbt schema YAML
        elif _is_dbt_schema(path):
            files.append(DiscoveredFile(path, "dbt_schema"))
        # dbt project config
        elif path.name == "dbt_project.yml":
            files.append(DiscoveredFile(path, "dbt_project"))
        # Raw SQL (not already tagged as dbt model)
        elif path.suffix.lower() == ".sql":
            files.append(DiscoveredFile(path, "sql"))
        # Python: Airflow vs Spark heuristics
        elif path.suffix.lower() == ".py":
            content = path.read_text(encoding="utf-8", errors="ignore")
            if _looks_like_airflow(content):
                files.append(DiscoveredFile(path, "airflow_dag"))
            elif _looks_like_spark(content):
                files.append(DiscoveredFile(path, "spark_job"))
        # YAML data contracts (not already dbt schema)
        elif path.suffix.lower() in (".yml", ".yaml") and _is_data_contract(path):
            files.append(DiscoveredFile(path, "data_contract"))

    return files


def _is_dbt_model(path: Path, root: Path) -> bool:
    """True if *.sql under a ``models`` directory (dbt layout heuristic)."""
    if path.suffix.lower() != ".sql":
        return False
    try:
        parts = path.relative_to(root).parts
    except ValueError:
        return False
    return "models" in parts


def _is_dbt_schema(path: Path) -> bool:
    if path.suffix.lower() not in (".yml", ".yaml"):
        return False
    name = path.name.lower()
    return "schema" in name or "sources" in name


def _looks_like_airflow(content: str) -> bool:
    return "from airflow" in content or "DAG(" in content


def _looks_like_spark(content: str) -> bool:
    return "SparkSession" in content or "spark.read" in content


def _is_data_contract(path: Path) -> bool:
    try:
        content = path.read_text(encoding="utf-8", errors="ignore")
    except OSError:
        return False
    return "dataContractSpecification" in content or "dataset:" in content
