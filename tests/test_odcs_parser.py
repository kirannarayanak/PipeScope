"""Tests for ODCS YAML parsing."""

from __future__ import annotations

from lineagescope.parsers.odcs_parser import ParsedContract, parse_odcs_file


def test_parse_legacy_dataset_schema() -> None:
    yml = """
dataContractSpecification: "0.9.0"
dataset:
  name: orders
  schema:
    - name: order_id
      type: string
    - name: amount
      type: decimal
"""
    out = parse_odcs_file("c.yaml", yml)
    assert len(out) == 1
    c = out[0]
    assert isinstance(c, ParsedContract)
    assert c.dataset_name == "orders"
    assert c.columns == {"order_id": "string", "amount": "decimal"}


def test_parse_odcs_v3_schema_list() -> None:
    yml = """
kind: DataContract
apiVersion: v3.1.0
schema:
  - name: tbl_a
    physicalType: table
    properties:
      - name: id
        logicalType: string
        physicalType: varchar(18)
      - name: n
        logicalType: integer
"""
    out = parse_odcs_file("v3.yaml", yml)
    assert len(out) == 1
    assert out[0].dataset_name == "tbl_a"
    assert out[0].columns["id"] == "string"
    assert out[0].columns["n"] == "integer"


def test_parse_empty_returns_empty() -> None:
    assert parse_odcs_file("x.yaml", "") == []
    assert parse_odcs_file("x.yaml", "foo: bar") == []


def test_v3_table_uses_physical_name_when_name_absent() -> None:
    yml = """
kind: DataContract
apiVersion: v3.1.0
schema:
  - physicalName: fact_only
    physicalType: table
    properties:
      - physicalName: pk_col
        logicalType: string
"""
    out = parse_odcs_file("c.yaml", yml)
    assert len(out) == 1
    assert out[0].dataset_name == "fact_only"
    assert "pk_col" in out[0].columns


def test_legacy_dataset_physical_name_and_schema_physical_name() -> None:
    yml = """
dataContractSpecification: "0.9.0"
dataset:
  physicalName: legacy_ds
  schema:
    - physicalName: col_a
      type: string
"""
    out = parse_odcs_file("l.yaml", yml)
    assert len(out) == 1
    assert out[0].dataset_name == "legacy_ds"
    assert out[0].columns["col_a"] == "string"
