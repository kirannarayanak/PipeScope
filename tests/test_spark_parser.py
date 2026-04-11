"""Tests for Spark AST parser."""

from pathlib import Path

from lineagescope.models import AssetType
from lineagescope.parsers.spark_parser import parse_spark_file


def test_spark_sql_extracts_table_refs() -> None:
    code = """
spark = None
spark.sql("SELECT o.id FROM warehouse.orders o JOIN raw.customers c ON o.cid = c.id")
"""
    assets, _edges = parse_spark_file("x.py", code)
    names = {a.name for a in assets}
    assert "warehouse.orders" in names
    assert "raw.customers" in names


def test_spark_parser_reads_join_write_and_edges() -> None:
    path = Path(__file__).resolve().parent / "fixtures" / "spark_sample_job.py"
    content = path.read_text(encoding="utf-8")
    assets, edges = parse_spark_file(str(path), content)

    names = {a.name for a in assets}
    assert names == {"db.orders", "s3://warehouse/customers", "db.orders_enriched"}
    assert all(a.asset_type == AssetType.TABLE for a in assets)

    edge_pairs = {(e.source, e.target) for e in edges}
    assert edge_pairs == {
        ("db.orders", "db.orders_enriched"),
        ("s3://warehouse/customers", "db.orders_enriched"),
    }
