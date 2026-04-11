"""Repository walker: classify data-related files by type."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class DiscoveredFile:
    """A file on disk and its inferred role."""

    path: Path
    file_type: str  # sql, dbt_model, dbt_schema, dbt_project, airflow_dag, spark_job, data_contract


def normalize_exclude_dir_names(raw: str | None) -> frozenset[str]:
    """Return lowercased directory basename tokens from a CLI ``--exclude`` string."""
    if not raw or not str(raw).strip():
        return frozenset()
    out: list[str] = []
    for chunk in str(raw).replace(";", ",").split(","):
        s = chunk.strip()
        if s:
            out.append(s.lower())
    return frozenset(out)


def scan_directory(
    root: Path,
    exclude_dir_names: frozenset[str] | None = None,
) -> list[DiscoveredFile]:
    """Walk *root* and classify SQL, dbt, Python DAGs/jobs, and contract YAML.

    Prunes hidden directories (``.*``) and any directory whose basename is in
    *exclude_dir_names* (compared case-insensitively).
    """
    root = root.resolve()
    if not root.is_dir():
        return []

    exc = exclude_dir_names or frozenset()
    files: list[DiscoveredFile] = []

    for dirpath, dirnames, filenames in os.walk(root, topdown=True, followlinks=False):
        dirnames[:] = [
            d
            for d in dirnames
            if not d.startswith(".") and d.lower() not in exc
        ]
        for name in filenames:
            if name.startswith("."):
                continue
            path = Path(dirpath) / name

            if _is_dbt_model(path, root):
                files.append(DiscoveredFile(path, "dbt_model"))
            elif _is_dbt_schema(path):
                files.append(DiscoveredFile(path, "dbt_schema"))
            elif name == "dbt_project.yml":
                files.append(DiscoveredFile(path, "dbt_project"))
            elif path.suffix.lower() == ".sql":
                files.append(DiscoveredFile(path, "sql"))
            elif path.suffix.lower() == ".py":
                try:
                    content = path.read_text(encoding="utf-8", errors="ignore")
                except OSError:
                    continue
                if _looks_like_airflow(content):
                    files.append(DiscoveredFile(path, "airflow_dag"))
                elif _looks_like_spark(content):
                    files.append(DiscoveredFile(path, "spark_job"))
            elif path.suffix.lower() in (".yml", ".yaml") and _is_data_contract(path):
                files.append(DiscoveredFile(path, "data_contract"))

    return files


def iter_file_paths_under(root: Path, exclude_dir_names: frozenset[str]) -> list[Path]:
    """Return non-hidden files under *root* using the same pruning as :func:`scan_directory`.

    Used to locate ``dbt_project.yml`` when discovery returns no project entries.
    """
    root = root.resolve()
    exc = exclude_dir_names or frozenset()
    out: list[Path] = []
    for dirpath, dirnames, filenames in os.walk(root, topdown=True, followlinks=False):
        dirnames[:] = [
            d
            for d in dirnames
            if not d.startswith(".") and d.lower() not in exc
        ]
        for name in filenames:
            if name.startswith("."):
                continue
            out.append(Path(dirpath) / name)
    return out


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
