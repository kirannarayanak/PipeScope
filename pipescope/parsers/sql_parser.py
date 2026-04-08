"""SQL parsing with SQLGlot: tables, edges, and static cost patterns."""

from __future__ import annotations

import sqlglot
from sqlglot import exp
from sqlglot.errors import ErrorLevel

from pipescope.models import Asset, AssetType, Edge


def parse_sql_file(
    file_path: str,
    content: str,
    dialect: str | None = None,
) -> tuple[list[Asset], list[Edge]]:
    """Parse a SQL file and extract assets + lineage edges."""
    assets: list[Asset] = []
    edges: list[Edge] = []

    try:
        statements = sqlglot.parse(
            content,
            read=dialect,
            error_level=ErrorLevel.IGNORE,
        )
    except Exception:
        return assets, edges

    for idx, stmt in enumerate(statements):
        if stmt is None:
            continue

        if isinstance(stmt, exp.Create):
            table_expr = _create_target_table(stmt)
            if table_expr is None:
                continue
            name = _qualified_name(table_expr)
            kind = str(stmt.args.get("kind", "")).upper()
            if "VIEW" in kind:
                asset_type = AssetType.VIEW
            elif "TABLE" in kind:
                asset_type = AssetType.TABLE
            else:
                continue
            columns = _extract_columns_from_create(stmt)
            assets.append(
                Asset(
                    name=name,
                    asset_type=asset_type,
                    file_path=file_path,
                    columns=columns,
                )
            )
            exclude = frozenset({name})
            for source_table in _extract_source_tables(stmt, exclude=exclude):
                edges.append(Edge(source=source_table, target=name))

        elif isinstance(stmt, exp.Insert):
            target_expr = stmt.this
            if isinstance(target_expr, exp.Schema):
                target_expr = target_expr.this
            if not isinstance(target_expr, exp.Table):
                continue
            target_name = _qualified_name(target_expr)
            exclude = frozenset({target_name})
            for source_table in _extract_source_tables(stmt, exclude=exclude):
                edges.append(Edge(source=source_table, target=target_name))

        elif isinstance(stmt, exp.Select):
            query_name = f"{file_path}#select{idx}"
            assets.append(
                Asset(
                    name=query_name,
                    asset_type=AssetType.VIEW,
                    file_path=file_path,
                    tags={"role": "query_block"},
                )
            )
            for source_table in _extract_source_tables(stmt):
                edges.append(Edge(source=source_table, target=query_name))

    return assets, edges


def _create_target_table(stmt: exp.Create) -> exp.Table | None:
    """Resolve the table being created."""
    this = stmt.this
    if isinstance(this, exp.Schema):
        inner = this.this
        if isinstance(inner, exp.Table):
            return inner
        return None
    if isinstance(this, exp.Table):
        return this
    return None


def _qualified_name(table: exp.Table) -> str:
    if getattr(table, "parts", None):
        return ".".join(p.name for p in table.parts)
    return str(table.name)


def _extract_source_tables(
    stmt: exp.Expression,
    *,
    exclude: frozenset[str] | None = None,
) -> list[str]:
    """Find all table references in *stmt* (optionally excluding target names)."""
    tables: list[str] = []
    seen: set[str] = set()
    exclude = exclude or frozenset()
    for table in stmt.find_all(exp.Table):
        name = _qualified_name(table)
        if not name or name in seen or name in exclude:
            continue
        seen.add(name)
        tables.append(name)
    return tables


def _extract_columns_from_create(stmt: exp.Expression) -> list[str]:
    """Column names from CREATE TABLE (...)."""
    columns: list[str] = []
    for col_def in stmt.find_all(exp.ColumnDef):
        col_name = col_def.name
        if isinstance(col_name, str):
            columns.append(col_name)
        elif isinstance(col_name, exp.Identifier):
            columns.append(col_name.name)
    return columns


def detect_cost_patterns(content: str, dialect: str | None = None) -> list[str]:
    """Detect expensive query patterns statically."""
    patterns: list[str] = []
    try:
        stmts = sqlglot.parse(
            content,
            read=dialect,
            error_level=ErrorLevel.IGNORE,
        )
    except Exception:
        return patterns

    for stmt in stmts:
        if stmt is None:
            continue

        for _ in stmt.find_all(exp.Star):
            patterns.append("SELECT_STAR")
            break

        for join in stmt.find_all(exp.Join):
            kind = (join.kind or "").upper()
            if kind == "CROSS" or "CROSS" in join.sql().upper():
                patterns.append("CROSS_JOIN")
                break

        if isinstance(stmt, (exp.Delete, exp.Update)) and not stmt.find(exp.Where):
            patterns.append("MISSING_WHERE_CLAUSE")

    # Preserve order, drop duplicates
    return list(dict.fromkeys(patterns))
