"""Tests for file scanner classification."""

from collections import Counter
from pathlib import Path

import pytest

from pipescope.scanner import DiscoveredFile, scan_directory

FIXTURE_ROOT = Path(__file__).resolve().parent / "fixtures" / "scanner_sample"


@pytest.fixture
def scanner_root() -> Path:
    assert FIXTURE_ROOT.is_dir(), f"Missing fixtures at {FIXTURE_ROOT}"
    return FIXTURE_ROOT


def test_scan_directory_classifies_sql_dbt_airflow_spark_contracts(scanner_root: Path) -> None:
    discovered = scan_directory(scanner_root)
    by_type = Counter(df.file_type for df in discovered)
    by_path = {df.path.resolve(): df.file_type for df in discovered}

    assert by_type["sql"] == 3
    assert by_type["dbt_model"] == 2
    assert by_type["dbt_schema"] == 1
    assert by_type["dbt_project"] == 1
    assert by_type["airflow_dag"] == 1
    assert by_type["spark_job"] == 1
    assert by_type["data_contract"] == 1

    adhoc_sql = {
        (scanner_root / "adhoc" / name).resolve()
        for name in ("a.sql", "b.sql", "c.sql")
    }
    for p in adhoc_sql:
        assert by_path[p] == "sql"

    dbt_models = {
        (scanner_root / "dbt_project" / "models" / "staging" / "stg_orders.sql").resolve(),
        (scanner_root / "dbt_project" / "models" / "marts" / "fct_revenue.sql").resolve(),
    }
    for p in dbt_models:
        assert by_path[p] == "dbt_model"

    assert (
        by_path[(scanner_root / "dbt_project" / "dbt_project.yml").resolve()]
        == "dbt_project"
    )
    assert (
        by_path[(scanner_root / "dbt_project" / "models" / "schema.yml").resolve()]
        == "dbt_schema"
    )
    assert (
        by_path[(scanner_root / "airflow" / "dags" / "sample_dag.py").resolve()]
        == "airflow_dag"
    )
    assert by_path[(scanner_root / "spark" / "batch_job.py").resolve()] == "spark_job"
    assert (
        by_path[(scanner_root / "contracts" / "orders_v1.yaml").resolve()]
        == "data_contract"
    )


def test_discovered_file_is_frozen_dataclass() -> None:
    p = Path("/tmp/x.sql")
    d = DiscoveredFile(p, "sql")
    assert d.path == p
    assert d.file_type == "sql"


def test_scan_directory_missing_root_returns_empty(tmp_path: Path) -> None:
    assert scan_directory(tmp_path / "nope") == []
