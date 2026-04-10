"""Run PipeScope on pinned real-world checkouts; write compact JSON summaries.

Usage (from repo root, dev env active):
    python real-project-tests/run_validation.py

Requires `pipescope` on PATH (e.g. pip install -e ".[dev]" from PipeScope root).

Optional: set ``PIPESCOPE_PRIVATE_SCAN_PATH`` to an extra directory (e.g. your work
repo). A scan is appended as ``private_workspace``; ``scan_root`` in JSON is redacted.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent
REPO_ROOT = ROOT.parent.resolve()
RESULTS = ROOT / "results"
PRIVATE_ENV = "PIPESCOPE_PRIVATE_SCAN_PATH"


def _pipescope_exe() -> str | None:
    w = shutil.which("pipescope")
    if w:
        return w
    scripts = Path(sys.executable).parent / "pipescope.exe"
    return str(scripts) if scripts.is_file() else None


def _display_scan_root(label: str, scan_path: Path, payload_root: str | None) -> str:
    if label == "private_workspace":
        return "(private path — redacted)"
    base = Path(payload_root or scan_path).resolve()
    try:
        return base.relative_to(REPO_ROOT).as_posix()
    except ValueError:
        return base.as_posix()


def _summarize(payload: dict) -> dict:
    graph = payload.get("graph") or {}
    findings = payload.get("findings") or []
    return {
        "version": payload.get("version"),
        "scan_root": payload.get("scan_root"),
        "discovered_file_count": payload.get("discovered_file_count"),
        "parsed_sql_file_count": payload.get("parsed_sql_file_count"),
        "parsed_airflow_file_count": payload.get("parsed_airflow_file_count"),
        "parsed_spark_file_count": payload.get("parsed_spark_file_count"),
        "parsed_dbt_project_count": payload.get("parsed_dbt_project_count"),
        "node_count": graph.get("node_count"),
        "edge_count": graph.get("edge_count"),
        "is_directed_acyclic": graph.get("is_directed_acyclic"),
        "findings_count": len(findings),
        "scores": payload.get("scores"),
    }


def _run_scan(name: str, scan_path: Path) -> tuple[int, dict | None, str | None]:
    exe = _pipescope_exe()
    if not exe:
        return 2, None, "pipescope executable not found"

    env = os.environ.copy()
    env.setdefault("PYTHONUTF8", "1")

    t0 = time.perf_counter()
    proc = subprocess.run(
        [exe, "scan", str(scan_path), "--format", "json"],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        env=env,
    )
    elapsed = round(time.perf_counter() - t0, 2)

    if proc.returncode != 0:
        err = (proc.stderr or proc.stdout or "")[:2000]
        return proc.returncode, None, err

    raw = proc.stdout.strip()
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as e:
        return 1, None, f"invalid JSON: {e}"

    summary = _summarize(payload)
    summary["scan_root"] = _display_scan_root(name, scan_path, payload.get("scan_root"))
    summary["scan_duration_seconds"] = elapsed
    summary["label"] = name

    RESULTS.mkdir(parents=True, exist_ok=True)
    out = RESULTS / f"{name}_summary.json"
    out.write_text(json.dumps(summary, indent=2), encoding="utf-8")

    return 0, summary, None


def main() -> int:
    targets: list[tuple[str, Path]] = [
        ("jaffle_shop", ROOT / "jaffle_shop"),
        ("dbt_artifacts", ROOT / "dbt_artifacts"),
        ("dbt_project_evaluator", ROOT / "dbt-project-evaluator"),
        (
            "airflow_example_dags",
            ROOT / "airflow" / "airflow-core" / "src" / "airflow" / "example_dags",
        ),
        ("spark_examples", ROOT / "spark" / "examples"),
    ]

    priv = os.environ.get(PRIVATE_ENV, "").strip()
    if priv:
        p = Path(os.path.expandvars(priv)).expanduser().resolve()
        if p.is_dir():
            targets.append(("private_workspace", p))
            print(
                f"NOTE optional {PRIVATE_ENV} -> extra scan private_workspace",
                flush=True,
            )
        else:
            print(
                f"WARN {PRIVATE_ENV} is set but not a directory: {priv!r}",
                flush=True,
            )

    failures: list[str] = []
    summaries: list[dict] = []

    for label, path in targets:
        if not path.is_dir():
            print(f"SKIP {label}: missing path {path}")
            failures.append(label)
            continue
        print(f"SCAN {label} -> {path} ...", flush=True)
        code, summary, err = _run_scan(label, path)
        if code != 0 or summary is None:
            print(f"FAIL {label} code={code} {err or ''}")
            failures.append(label)
            continue
        print(
            f"OK   {label} assets~={summary.get('node_count')} "
            f"findings={summary.get('findings_count')} "
            f"{summary.get('scan_duration_seconds')}s",
            flush=True,
        )
        summaries.append(summary)

    index = {
        "generated_by": "real-project-tests/run_validation.py",
        "targets_ok": [s["label"] for s in summaries],
        "targets_failed": failures,
        "summaries": summaries,
    }
    RESULTS.mkdir(parents=True, exist_ok=True)
    (RESULTS / "index.json").write_text(json.dumps(index, indent=2), encoding="utf-8")

    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())
