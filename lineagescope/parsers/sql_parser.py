"""SQL parsing with SQLGlot: tables, edges, and static cost patterns."""

from __future__ import annotations

import sqlglot
from sqlglot import exp
from sqlglot.errors import ErrorLevel

from lineagescope.models import Asset, AssetType, Edge


def _sql_file_has_leading_documentation(content: str) -> bool:
    """True when the file opens with ``/* */`` or ``--`` comments (dbt/SQL style docs)."""
    s = content.lstrip("\ufeff \t\n\r")
    if not s:
        return False
    if s.startswith("/*"):
        end = s.find("*/")
        if end == -1:
            return False
        return len(s[2:end].strip()) > 0
    for line in s.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        if stripped.startswith("--"):
            return len(stripped) > 2
        break
    return False


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

    file_has_docs = _sql_file_has_leading_documentation(content)

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
            columns, column_types = _extract_columns_and_types_from_create(stmt)
            assets.append(
                Asset(
                    name=name,
                    asset_type=asset_type,
                    file_path=file_path,
                    columns=columns,
                    column_types=column_types,
                    has_docs=file_has_docs,
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
                    has_docs=file_has_docs,
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


def _extract_columns_and_types_from_create(
    stmt: exp.Expression,
) -> tuple[list[str], dict[str, str]]:
    """Column names and SQL types from ``CREATE TABLE`` / ``CREATE VIEW`` column defs."""
    columns: list[str] = []
    types: dict[str, str] = {}
    for col_def in stmt.find_all(exp.ColumnDef):
        col_name = col_def.name
        if isinstance(col_name, exp.Identifier):
            col_name = col_name.name
        elif not isinstance(col_name, str):
            continue
        columns.append(col_name)
        kind = col_def.args.get("kind")
        if kind is not None:
            types[col_name] = kind.sql()
    return columns, types


def _extract_columns_from_create(stmt: exp.Expression) -> list[str]:
    """Column names from CREATE TABLE (...)."""
    cols, _types = _extract_columns_and_types_from_create(stmt)
    return cols


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

        if isinstance(stmt, exp.Delete | exp.Update) and not stmt.find(exp.Where):
            patterns.append("MISSING_WHERE_CLAUSE")

        if isinstance(stmt, exp.Select):
            if stmt.find(exp.From) and not stmt.find(exp.Where):
                patterns.append("SELECT_WITHOUT_WHERE")
            if stmt.find(exp.From) and not stmt.args.get("limit") and not stmt.args.get("fetch"):
                patterns.append("NO_LIMIT")

    # Preserve order, drop duplicates
    return list(dict.fromkeys(patterns))


def _resolve_partition_column(table_name: str, partition_map: dict[str, str]) -> str | None:
    if table_name in partition_map:
        return partition_map[table_name]
    stem = table_name.split(".")[-1]
    for key, col in partition_map.items():
        if key.split(".")[-1].lower() == stem.lower():
            return col
    return None


def _expr_references_column(where_expr: exp.Expression | None, col_name: str) -> bool:
    if where_expr is None:
        return False
    c_low = col_name.lower()
    for col in where_expr.find_all(exp.Column):
        if col.name.lower() == c_low:
            return True
    return False


def detect_partition_filter_issues(
    content: str,
    dialect: str | None,
    partition_map: dict[str, str],
) -> list[str]:
    """Flag ``MISSING_PARTITION_FILTER`` when a partitioned table lacks a ``WHERE`` on that key."""
    if not partition_map:
        return []
    issues: list[str] = []
    try:
        stmts = sqlglot.parse(
            content,
            read=dialect,
            error_level=ErrorLevel.IGNORE,
        )
    except Exception:
        return issues

    for stmt in stmts:
        if stmt is None:
            continue
        for sel in stmt.find_all(exp.Select):
            if not isinstance(sel, exp.Select) or not sel.find(exp.From):
                continue
            for table in sel.find_all(exp.Table):
                tname = _qualified_name(table)
                pcol = _resolve_partition_column(tname, partition_map)
                if not pcol:
                    continue
                where = sel.args.get("where")
                if isinstance(where, exp.Where):
                    wexpr = where.this
                else:
                    wexpr = where
                if not _expr_references_column(wexpr, pcol):
                    issues.append("MISSING_PARTITION_FILTER")
    return list(dict.fromkeys(issues))
