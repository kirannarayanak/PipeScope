"""dbt project parsing without dbt runtime or warehouse connection."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path

import sqlglot
import yaml
from sqlglot import exp
from sqlglot.errors import ErrorLevel

from pipescope.models import Asset, AssetType, Edge

REF_PATTERN = re.compile(r"\{\{\s*ref\(['\"]([^'\"]+)['\"]\)\s*\}\}")
SOURCE_PATTERN = re.compile(r"\{\{\s*source\(['\"]([^'\"]+)['\"],\s*['\"]([^'\"]+)['\"]\)\s*\}\}")
CONFIG_PATTERN = re.compile(r"\{\{\s*config\((.+?)\)\s*\}\}", re.DOTALL)

JINJA_COMMENT_PATTERN = re.compile(r"\{#.*?#\}", re.DOTALL)
JINJA_BLOCK_PATTERN = re.compile(r"\{%.*?%\}", re.DOTALL)
JINJA_EXPR_PATTERN = re.compile(r"\{\{.*?\}\}", re.DOTALL)


@dataclass
class ModelMeta:
    """Schema-level metadata for a dbt model."""

    has_docs: bool = False
    has_tests: bool = False
    description: str | None = None
    columns: list[str] = field(default_factory=list)


def parse_dbt_project(root: Path) -> tuple[list[Asset], list[Edge]]:
    """Parse dbt files under *root* into PipeScope assets and edges."""
    root = root.resolve()
    project_file = root / "dbt_project.yml"
    if not project_file.is_file():
        return [], []

    project_cfg = _safe_yaml_dict(project_file)
    project_name = str(project_cfg.get("name", root.name))
    model_paths_raw = project_cfg.get("model-paths", ["models"])
    model_paths = _normalize_model_paths(model_paths_raw)

    model_meta, source_assets = _collect_schema_metadata(root, model_paths)
    assets: list[Asset] = []
    assets.extend(source_assets.values())
    edges: list[Edge] = []
    seen_edges: set[tuple[str, str]] = set()

    for model_sql in _iter_model_sql_files(root, model_paths):
        model_name = model_sql.stem
        rel_path = str(model_sql.relative_to(root))
        meta = model_meta.get(model_name, ModelMeta())
        asset = Asset(
            name=model_name,
            asset_type=AssetType.DBT_MODEL,
            file_path=rel_path,
            columns=list(meta.columns),
            has_docs=meta.has_docs,
            has_tests=meta.has_tests,
            description=meta.description,
            tags={"project": project_name},
        )
        assets.append(asset)

        raw_sql = model_sql.read_text(encoding="utf-8", errors="ignore")

        for ref_name in _extract_refs(raw_sql):
            _add_edge_once(edges, seen_edges, source=ref_name, target=model_name)

        for source_name, table_name in _extract_sources(raw_sql):
            source_asset_name = f"{source_name}.{table_name}"
            if source_asset_name not in source_assets:
                source_assets[source_asset_name] = Asset(
                    name=source_asset_name,
                    asset_type=AssetType.DBT_SOURCE,
                    file_path=rel_path,
                    tags={"project": project_name, "defined_from": "source_call"},
                )
                assets.append(source_assets[source_asset_name])
            _add_edge_once(edges, seen_edges, source=source_asset_name, target=model_name)

        cleaned_sql = _strip_jinja(raw_sql)
        for table_name in _extract_sql_tables(cleaned_sql):
            if table_name == model_name:
                continue
            _add_edge_once(edges, seen_edges, source=table_name, target=model_name)

    return assets, edges


def _normalize_model_paths(raw: object) -> list[str]:
    if isinstance(raw, list):
        return [str(p) for p in raw if isinstance(p, str)]
    if isinstance(raw, str):
        return [raw]
    return ["models"]


def _iter_model_sql_files(root: Path, model_paths: list[str]) -> list[Path]:
    files: list[Path] = []
    for rel in model_paths:
        model_root = root / rel
        if not model_root.is_dir():
            continue
        files.extend(sorted(model_root.rglob("*.sql")))
    return files


def _collect_schema_metadata(
    root: Path,
    model_paths: list[str],
) -> tuple[dict[str, ModelMeta], dict[str, Asset]]:
    meta_by_model: dict[str, ModelMeta] = {}
    sources: dict[str, Asset] = {}

    for rel in model_paths:
        model_root = root / rel
        if not model_root.is_dir():
            continue
        yaml_files = list(model_root.rglob("*.yml")) + list(model_root.rglob("*.yaml"))
        for yml_path in sorted(yaml_files):
            payload = _safe_yaml_dict(yml_path)
            if not payload:
                continue
            _merge_model_meta(payload, meta_by_model)
            _collect_source_assets(payload, yml_path, root, sources)

    return meta_by_model, sources


def _merge_model_meta(payload: dict, meta_by_model: dict[str, ModelMeta]) -> None:
    models = payload.get("models")
    if not isinstance(models, list):
        return

    for entry in models:
        if not isinstance(entry, dict):
            continue
        name = entry.get("name")
        if not isinstance(name, str):
            continue
        meta = meta_by_model.setdefault(name, ModelMeta())

        desc = entry.get("description")
        if isinstance(desc, str) and desc.strip():
            meta.description = desc
            meta.has_docs = True

        model_tests = entry.get("tests")
        if isinstance(model_tests, list) and model_tests:
            meta.has_tests = True

        columns = entry.get("columns")
        if not isinstance(columns, list):
            continue
        for col in columns:
            if not isinstance(col, dict):
                continue
            col_name = col.get("name")
            if isinstance(col_name, str) and col_name not in meta.columns:
                meta.columns.append(col_name)
            col_desc = col.get("description")
            if isinstance(col_desc, str) and col_desc.strip():
                meta.has_docs = True
            col_tests = col.get("tests")
            if isinstance(col_tests, list) and col_tests:
                meta.has_tests = True


def _collect_source_assets(
    payload: dict,
    yml_path: Path,
    root: Path,
    out: dict[str, Asset],
) -> None:
    sources = payload.get("sources")
    if not isinstance(sources, list):
        return

    rel_path = str(yml_path.relative_to(root))
    for source_entry in sources:
        if not isinstance(source_entry, dict):
            continue
        source_name = source_entry.get("name")
        tables = source_entry.get("tables")
        if not isinstance(source_name, str) or not isinstance(tables, list):
            continue
        for table in tables:
            if not isinstance(table, dict):
                continue
            table_name = table.get("name")
            if not isinstance(table_name, str):
                continue
            asset_name = f"{source_name}.{table_name}"
            description = table.get("description")
            has_docs = isinstance(description, str) and bool(description.strip())
            has_tests = isinstance(table.get("tests"), list) and bool(table.get("tests"))
            columns: list[str] = []
            if isinstance(table.get("columns"), list):
                for col in table["columns"]:
                    if isinstance(col, dict) and isinstance(col.get("name"), str):
                        columns.append(col["name"])
                        cdesc = col.get("description")
                        if isinstance(cdesc, str) and cdesc.strip():
                            has_docs = True
                        ctests = col.get("tests")
                        if isinstance(ctests, list) and ctests:
                            has_tests = True

            out[asset_name] = Asset(
                name=asset_name,
                asset_type=AssetType.DBT_SOURCE,
                file_path=rel_path,
                columns=columns,
                has_docs=has_docs,
                has_tests=has_tests,
                description=description if isinstance(description, str) else None,
            )


def _extract_refs(sql: str) -> list[str]:
    return list(dict.fromkeys(m.group(1) for m in REF_PATTERN.finditer(sql)))


def _extract_sources(sql: str) -> list[tuple[str, str]]:
    return list(dict.fromkeys((m.group(1), m.group(2)) for m in SOURCE_PATTERN.finditer(sql)))


def _strip_jinja(sql: str) -> str:
    stripped = CONFIG_PATTERN.sub(" ", sql)

    def _ref_repl(m: re.Match[str]) -> str:
        return m.group(1)

    def _source_repl(m: re.Match[str]) -> str:
        return f"{m.group(1)}.{m.group(2)}"

    stripped = REF_PATTERN.sub(_ref_repl, stripped)
    stripped = SOURCE_PATTERN.sub(_source_repl, stripped)
    stripped = JINJA_COMMENT_PATTERN.sub(" ", stripped)
    stripped = JINJA_BLOCK_PATTERN.sub(" ", stripped)
    stripped = JINJA_EXPR_PATTERN.sub(" ", stripped)
    return stripped


def _extract_sql_tables(sql: str) -> list[str]:
    try:
        statements = sqlglot.parse(sql, error_level=ErrorLevel.IGNORE)
    except Exception:
        return []

    found: list[str] = []
    seen: set[str] = set()
    for stmt in statements:
        if stmt is None:
            continue
        for table in stmt.find_all(exp.Table):
            name = _table_name(table)
            if not name or name in seen:
                continue
            seen.add(name)
            found.append(name)
    return found


def _table_name(table: exp.Table) -> str:
    parts = getattr(table, "parts", None)
    if parts:
        return ".".join(p.name for p in parts)
    return str(table.name)


def _add_edge_once(
    edges: list[Edge],
    seen: set[tuple[str, str]],
    *,
    source: str,
    target: str,
) -> None:
    pair = (source, target)
    if pair in seen:
        return
    seen.add(pair)
    edges.append(Edge(source=source, target=target))


def _safe_yaml_dict(path: Path) -> dict:
    try:
        raw = yaml.safe_load(path.read_text(encoding="utf-8", errors="ignore"))
    except (OSError, yaml.YAMLError):
        return {}
    return raw if isinstance(raw, dict) else {}
