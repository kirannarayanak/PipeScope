"""Tests for SQL parser."""

from pathlib import Path

from pipescope.parsers import sql_parser


def test_parse_sql_file_reads(tmp_path: Path) -> None:
    f = tmp_path / "q.sql"
    f.write_text("SELECT 1 AS x;", encoding="utf-8")
    sql_parser.parse_sql_file(f)
