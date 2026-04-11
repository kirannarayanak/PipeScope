"""Open Data Contract Standard (ODCS) YAML: dataset tables and column expectations."""

from __future__ import annotations

from dataclasses import dataclass, field

import yaml


@dataclass(frozen=True)
class ParsedContract:
    """One logical dataset/table block from a contract file (ODCS v3 or legacy 0.9-style)."""

    file_path: str
    dataset_name: str
    columns: dict[str, str | None] = field(default_factory=dict)


def parse_odcs_file(file_path: str, content: str) -> list[ParsedContract]:
    """Parse ODCS YAML into zero or more :class:`ParsedContract` tables with column specs."""
    try:
        payload = yaml.safe_load(content)
    except yaml.YAMLError:
        return []
    if not isinstance(payload, dict):
        return []
    out: list[ParsedContract] = []
    out.extend(_parse_odcs_v3_schema(payload, file_path))
    if not out:
        out.extend(_parse_legacy_dataset(payload, file_path))
    return out


def _first_non_empty_str(d: dict, keys: tuple[str, ...]) -> str | None:
    """Return the first string value for *keys* that is non-empty after strip."""
    for k in keys:
        v = d.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()
    return None


def _parse_odcs_v3_schema(payload: dict, file_path: str) -> list[ParsedContract]:
    """ODCS v3: ``schema:`` is a list of objects with ``name`` and ``properties``."""
    schema = payload.get("schema")
    if not isinstance(schema, list):
        return []
    out: list[ParsedContract] = []
    for item in schema:
        if not isinstance(item, dict):
            continue
        name = _first_non_empty_str(
            item,
            ("name", "physicalName", "logicalName"),
        )
        if not name:
            continue
        cols: dict[str, str | None] = {}
        props = item.get("properties")
        if isinstance(props, list):
            for p in props:
                if not isinstance(p, dict):
                    continue
                cn = _first_non_empty_str(
                    p,
                    ("name", "physicalName", "logicalName"),
                )
                if not cn:
                    continue
                lt = p.get("logicalType")
                pt = p.get("physicalType")
                ty: str | None = None
                if isinstance(lt, str) and lt.strip():
                    ty = lt.strip()
                elif isinstance(pt, str) and pt.strip():
                    ty = pt.strip()
                cols[cn] = ty
        if cols:
            out.append(
                ParsedContract(
                    file_path=file_path,
                    dataset_name=name,
                    columns=cols,
                )
            )
    return out


def _parse_legacy_dataset(payload: dict, file_path: str) -> list[ParsedContract]:
    """Legacy ``dataContractSpecification`` + ``dataset`` with optional ``schema`` list."""
    dataset = payload.get("dataset")
    if not isinstance(dataset, dict):
        return []
    ds_name = _first_non_empty_str(
        dataset,
        ("name", "physicalName", "logicalName"),
    )
    if not ds_name:
        return []
    schema = dataset.get("schema")
    cols: dict[str, str | None] = {}
    if isinstance(schema, list):
        for row in schema:
            if not isinstance(row, dict):
                continue
            cn = _first_non_empty_str(
                row,
                ("name", "physicalName", "logicalName"),
            )
            if not cn:
                continue
            ty = (
                row.get("type")
                or row.get("logicalType")
                or row.get("physicalType")
                or row.get("dataType")
            )
            cols[cn] = ty.strip() if isinstance(ty, str) and ty.strip() else None
    if not cols:
        return []
    return [
        ParsedContract(
            file_path=file_path,
            dataset_name=ds_name,
            columns=cols,
        )
    ]
