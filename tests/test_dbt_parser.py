"""Tests for dbt parser."""

from pathlib import Path

from pipescope.parsers import dbt_parser


def test_parse_dbt_project_accepts_path(tmp_path: Path) -> None:
    dbt_parser.parse_dbt_project(tmp_path)
