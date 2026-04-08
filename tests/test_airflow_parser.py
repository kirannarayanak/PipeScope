"""Tests for Airflow AST parser."""

from pathlib import Path

from pipescope.models import AssetType
from pipescope.parsers.airflow_parser import parse_airflow_file


def test_parse_airflow_sample_dag_extracts_tasks_and_dependencies() -> None:
    dag_path = Path(__file__).resolve().parent / "fixtures" / "airflow_sample_dag.py"
    content = dag_path.read_text(encoding="utf-8")
    assets, edges = parse_airflow_file(str(dag_path), content)

    dag_assets = [a for a in assets if a.asset_type == AssetType.AIRFLOW_DAG]
    task_assets = [a for a in assets if a.asset_type == AssetType.AIRFLOW_TASK]
    assert [a.name for a in dag_assets] == ["sample_pipeline"]
    assert len(task_assets) == 4

    task_names = {a.name for a in task_assets}
    assert task_names == {
        "sample_pipeline.extract",
        "sample_pipeline.transform",
        "sample_pipeline.load",
        "sample_pipeline.notify",
    }

    load = next(a for a in task_assets if a.name == "sample_pipeline.load")
    assert load.tags.get("operator") == "BigQueryInsertJobOperator"
    assert load.tags.get("sql") == "select * from analytics.table_a"

    edge_pairs = {(e.source, e.target) for e in edges}
    assert ("sample_pipeline", "sample_pipeline.extract") in edge_pairs
    assert ("sample_pipeline", "sample_pipeline.transform") in edge_pairs
    assert ("sample_pipeline", "sample_pipeline.load") in edge_pairs
    assert ("sample_pipeline", "sample_pipeline.notify") in edge_pairs
    assert ("sample_pipeline.extract", "sample_pipeline.transform") in edge_pairs
    assert ("sample_pipeline.transform", "sample_pipeline.load") in edge_pairs
    assert ("sample_pipeline.extract", "sample_pipeline.notify") in edge_pairs
