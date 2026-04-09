"""Spark job parsing via Python AST (no Spark runtime required)."""

from __future__ import annotations

import ast
from typing import Literal

import sqlglot
from sqlglot import exp
from sqlglot.errors import ErrorLevel

from pipescope.models import Asset, AssetType, Edge


def parse_spark_file(file_path: str, content: str) -> tuple[list[Asset], list[Edge]]:
    """Parse a PySpark file and extract table/path reads, writes, and lineage edges."""
    try:
        tree = ast.parse(content, filename=file_path)
    except SyntaxError:
        return [], []

    mod_doc = ast.get_docstring(tree)
    file_has_docs = bool(mod_doc and mod_doc.strip())
    visitor = SparkVisitor(file_path=file_path, file_has_docs=file_has_docs)
    visitor.visit(tree)
    return visitor.to_assets_edges()


class SparkVisitor(ast.NodeVisitor):
    """Collect Spark read/write targets and derive simple read→write edges."""

    def __init__(self, file_path: str, *, file_has_docs: bool = False) -> None:
        self.file_path = file_path
        self.file_has_docs = file_has_docs
        self.reads: list[str] = []
        self.writes: list[str] = []
        self._seen_reads: set[str] = set()
        self._seen_writes: set[str] = set()

    def visit_Call(self, node: ast.Call) -> None:
        chain = _method_chain_name(node)
        if not chain:
            self.generic_visit(node)
            return

        if chain.startswith("spark.read") or chain.startswith("spark.table"):
            for target in _extract_read_targets(chain, node):
                self._add_read(target)
        elif chain == "spark.sql":
            for target in _tables_from_sql_arg(node):
                self._add_read(target)
        elif ".write." in chain or chain.endswith(".write"):
            for target in _extract_write_targets(chain, node):
                self._add_write(target)

        self.generic_visit(node)

    def _add_read(self, name: str) -> None:
        if name and name not in self._seen_reads:
            self._seen_reads.add(name)
            self.reads.append(name)

    def _add_write(self, name: str) -> None:
        if name and name not in self._seen_writes:
            self._seen_writes.add(name)
            self.writes.append(name)

    def to_assets_edges(self) -> tuple[list[Asset], list[Edge]]:
        assets: list[Asset] = []
        edges: list[Edge] = []
        all_ids = set(self.reads) | set(self.writes)

        for name in sorted(all_ids):
            role: Literal["read", "write"] = "write" if name in self._seen_writes else "read"
            assets.append(
                Asset(
                    name=name,
                    asset_type=AssetType.TABLE,
                    file_path=self.file_path,
                    has_docs=self.file_has_docs,
                    tags={"spark_role": role},
                )
            )

        if len(self.writes) == 1 and self.reads:
            out = self.writes[0]
            for src in self.reads:
                edges.append(Edge(source=src, target=out))
        elif len(self.writes) > 1 and self.reads:
            for w in self.writes:
                for src in self.reads:
                    edges.append(Edge(source=src, target=w))

        return assets, edges


def _method_chain_name(call: ast.Call) -> str | None:
    """Build names like ``spark.read.parquet`` or ``df.write.saveAsTable``."""
    parts: list[str] = []
    node: ast.AST = call.func
    while isinstance(node, ast.Attribute):
        parts.append(node.attr)
        node = node.value
    if isinstance(node, ast.Call):
        inner = _method_chain_name(node)
        tail = ".".join(reversed(parts)) if parts else ""
        if inner and tail:
            return f"{inner}.{tail}"
        return inner or tail or None
    if isinstance(node, ast.Name):
        parts.append(node.id)
    return ".".join(reversed(parts)) if parts else None


def _first_str_arg(call: ast.Call) -> str | None:
    if not call.args:
        return None
    arg0 = call.args[0]
    if isinstance(arg0, ast.Constant) and isinstance(arg0.value, str):
        return arg0.value
    return None


def _extract_read_targets(chain: str, call: ast.Call) -> list[str]:
    if chain == "spark.table":
        s = _first_str_arg(call)
        return [s] if s else []

    if chain.endswith(".load"):
        s = _first_str_arg(call)
        return [s] if s else []

    if chain.endswith(".parquet") or chain.endswith(".csv") or chain.endswith(".json"):
        s = _first_str_arg(call)
        return [s] if s else []

    if chain.endswith(".table") and "read" in chain:
        s = _first_str_arg(call)
        return [s] if s else []

    if ".format" in chain and chain.endswith(".load"):
        s = _first_str_arg(call)
        return [s] if s else []

    return []


def _extract_write_targets(chain: str, call: ast.Call) -> list[str]:
    if chain.endswith(".saveAsTable") or chain.endswith(".insertInto"):
        s = _first_str_arg(call)
        return [s] if s else []
    if chain.endswith(".parquet") or chain.endswith(".csv") or chain.endswith(".json"):
        s = _first_str_arg(call)
        return [s] if s else []
    if chain.endswith(".save"):
        s = _first_str_arg(call)
        return [s] if s else []
    return []


def _tables_from_sql_arg(call: ast.Call) -> list[str]:
    sql = _first_str_arg(call)
    if not sql:
        return []
    try:
        statements = sqlglot.parse(sql, error_level=ErrorLevel.IGNORE)
    except Exception:
        return []
    out: list[str] = []
    seen: set[str] = set()
    for stmt in statements:
        if stmt is None:
            continue
        for table in stmt.find_all(exp.Table):
            name = _sql_table_name(table)
            if name and name not in seen:
                seen.add(name)
                out.append(name)
    return out


def _sql_table_name(table: exp.Table) -> str:
    if getattr(table, "parts", None):
        return ".".join(p.name for p in table.parts)
    return str(table.name)

