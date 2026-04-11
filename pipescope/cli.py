"""Typer CLI entry point for PipeScope."""

from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import urllib.error
import urllib.request
from datetime import UTC, datetime, timedelta
from pathlib import Path

import typer
from rich.console import Console
from rich.panel import Panel
from rich.progress import (
    BarColumn,
    Progress,
    SpinnerColumn,
    TaskProgressColumn,
    TextColumn,
    TimeElapsedColumn,
)
from rich.table import Table

from pipescope import __version__
from pipescope.analyzers.complexity import analyze_complexity
from pipescope.analyzers.contracts import analyze_contract_compliance
from pipescope.analyzers.cost_hotspots import analyze_cost_hotspots
from pipescope.analyzers.dead_assets import (
    analyze_dead_assets,
    parse_dead_asset_terminal_tags_cli,
    parse_dead_asset_whitelist_cli,
)
from pipescope.analyzers.doc_coverage import analyze_documentation_coverage
from pipescope.analyzers.ownership import analyze_ownership
from pipescope.analyzers.test_coverage import analyze_test_coverage
from pipescope.graph import build_pipeline_graph, compute_scan_analytics, graph_summary
from pipescope.models import Asset, Edge
from pipescope.parsers import parse_dbt_project, parse_file, parse_odcs_file
from pipescope.parsers.odcs_parser import ParsedContract
from pipescope.reporters.html_report import write_report
from pipescope.reporters.json_report import format_scan_json
from pipescope.reporters.terminal import print_terminal_report
from pipescope.scanner import (
    DiscoveredFile,
    iter_file_paths_under,
    normalize_exclude_dir_names,
    scan_directory,
)

app = typer.Typer(
    name="pipescope",
    help="PipeScope: Universal static analyzer for data pipelines.",
    no_args_is_help=True,
)

def _ensure_utf8_stdio() -> None:
    """Prefer UTF-8 on Windows so Rich tables and paths print reliably."""
    if sys.platform != "win32":
        return
    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if callable(reconfigure):
            try:
                reconfigure(encoding="utf-8")
            except (OSError, ValueError, AttributeError):
                pass


def _make_console() -> Console:
    """Terminal-aware width: full width when interactive, wider default when captured."""
    width: int | None = None
    try:
        if hasattr(sys.stdout, "isatty") and sys.stdout.isatty():
            width = shutil.get_terminal_size().columns
        else:
            width = 120
    except OSError:
        width = 120
    return Console(
        width=width,
        legacy_windows=False,
    )


@app.callback()
def main() -> None:
    """PipeScope: scan repos, diff scans across git refs, or run CI gates.

    Examples:

        pipescope scan . --format json

        pipescope diff HEAD~1 --path .

        pipescope ci --threshold 70 --path .
    """


def _relative_file_path(file_path: Path, root: Path) -> str:
    try:
        return str(file_path.resolve().relative_to(root.resolve()))
    except ValueError:
        return str(file_path)


def _collect_dbt_project_roots(
    scan_root: Path,
    files: list[DiscoveredFile],
    exclude_names: frozenset[str],
) -> list[Path]:
    """Directory paths that contain ``dbt_project.yml`` (one dbt project each)."""
    roots: set[Path] = set()
    for f in files:
        if f.file_type == "dbt_project":
            roots.add(f.path.resolve().parent)
    if not roots:
        for path in iter_file_paths_under(scan_root, exclude_names):
            if path.name == "dbt_project.yml" and path.is_file():
                roots.add(path.resolve().parent)
    return sorted(roots)


def _path_under_any_project(path: Path, project_roots: set[Path]) -> bool:
    rp = path.resolve()
    for pr in project_roots:
        try:
            rp.relative_to(pr)
            return True
        except ValueError:
            continue
    return False


def _remap_asset_paths_to_scan_root(
    assets: list,
    project_root: Path,
    scan_root: Path,
) -> None:
    """Rewrite ``file_path`` from dbt-project-relative to scan-root-relative."""
    scan_root = scan_root.resolve()
    project_root = project_root.resolve()
    for asset in assets:
        rel = Path(asset.file_path)
        if rel.is_absolute():
            full = rel
        else:
            full = (project_root / rel).resolve()
        try:
            asset.file_path = str(full.relative_to(scan_root))
        except ValueError:
            asset.file_path = str(full)


def _dedupe_edges(edges: list[Edge]) -> list[Edge]:
    seen: set[tuple[str, str]] = set()
    out: list[Edge] = []
    for e in edges:
        k = (e.source, e.target)
        if k in seen:
            continue
        seen.add(k)
        out.append(e)
    return out


def collect_scan(
    root: Path,
    dialect: str | None,
    *,
    exclude_names: frozenset[str] = frozenset(),
    parse_warnings: list[str] | None = None,
    progress: Progress | None = None,
) -> tuple[list[DiscoveredFile], list[Asset], list[Edge], list[ParsedContract]]:
    """Discover files and parse SQL, dbt projects, Airflow DAGs, Spark jobs, and ODCS contracts."""
    scan_root = root.resolve()
    warns: list[str] = parse_warnings if parse_warnings is not None else []

    def _warn(msg: str) -> None:
        warns.append(msg)

    def _advance(task_id: int | None) -> None:
        if progress is not None and task_id is not None:
            progress.advance(task_id)

    t_walk: int | None = None
    if progress is not None:
        t_walk = progress.add_task("Walking repository", total=1)
    files = scan_directory(scan_root, exclude_names)
    _advance(t_walk)

    dbt_roots_list = _collect_dbt_project_roots(scan_root, files, exclude_names)
    dbt_roots = set(dbt_roots_list)

    all_assets: list[Asset] = []
    all_edges: list[Edge] = []
    parsed_contracts: list[ParsedContract] = []

    t_dbt: int | None = None
    if progress is not None:
        t_dbt = progress.add_task(
            "dbt projects",
            total=max(1, len(dbt_roots_list)),
        )
    for project_root in dbt_roots_list:
        try:
            dbt_assets, dbt_edges = parse_dbt_project(
                project_root,
                exclude_dir_names=exclude_names,
            )
        except Exception as ex:
            _warn(
                f"dbt project {project_root.name}: skipped ({type(ex).__name__}: {ex})",
            )
            dbt_assets, dbt_edges = [], []
        _remap_asset_paths_to_scan_root(dbt_assets, project_root, scan_root)
        all_assets.extend(dbt_assets)
        all_edges.extend(dbt_edges)
        _advance(t_dbt)
    if progress is not None and t_dbt is not None and not dbt_roots_list:
        progress.advance(t_dbt)

    to_parse = [
        f
        for f in files
        if f.file_type in (
            "sql",
            "dbt_model",
            "dbt_schema",
            "airflow_dag",
            "spark_job",
        )
        and not (
            f.file_type in ("dbt_model", "dbt_schema")
            and _path_under_any_project(f.path, dbt_roots)
        )
    ]

    t_parse: int | None = None
    if progress is not None:
        t_parse = progress.add_task(
            "Parsing source files",
            total=max(1, len(to_parse)),
        )
    for f in to_parse:
        rel = _relative_file_path(f.path, scan_root)
        try:
            assets, edges = parse_file(f, dialect, scan_root=scan_root)
        except Exception as ex:
            _warn(f"{rel}: skipped ({type(ex).__name__}: {ex})")
            assets, edges = [], []
        all_assets.extend(assets)
        all_edges.extend(edges)
        _advance(t_parse)
    if progress is not None and t_parse is not None and not to_parse:
        progress.advance(t_parse)

    contract_files = [f for f in files if f.file_type == "data_contract"]
    t_contract: int | None = None
    if progress is not None:
        t_contract = progress.add_task(
            "Data contracts",
            total=max(1, len(contract_files)),
        )
    for f in contract_files:
        rel = _relative_file_path(f.path, scan_root)
        try:
            content = f.path.read_text(encoding="utf-8", errors="ignore")
        except OSError as ex:
            _warn(f"{rel}: skipped (read error: {ex})")
            _advance(t_contract)
            continue
        try:
            parsed_contracts.extend(parse_odcs_file(rel, content))
        except Exception as ex:
            _warn(f"{rel}: skipped ({type(ex).__name__}: {ex})")
        _advance(t_contract)
    if progress is not None and t_contract is not None and not contract_files:
        progress.advance(t_contract)

    all_edges = _dedupe_edges(all_edges)
    return files, all_assets, all_edges, parsed_contracts


def _parsed_sql_file_count(files: list[DiscoveredFile], dbt_roots: set[Path]) -> int:
    n = 0
    for f in files:
        if f.file_type not in ("sql", "dbt_model"):
            continue
        if f.file_type == "dbt_model" and _path_under_any_project(f.path, dbt_roots):
            continue
        n += 1
    return n


def _parsed_airflow_file_count(files: list[DiscoveredFile]) -> int:
    return sum(1 for f in files if f.file_type == "airflow_dag")


def _parsed_spark_file_count(files: list[DiscoveredFile]) -> int:
    return sum(1 for f in files if f.file_type == "spark_job")


def _parsed_dbt_project_count(dbt_roots: list[Path]) -> int:
    return len(dbt_roots)


def _build_scan_payload_dict(
    *,
    version: str,
    scan_root: Path,
    files: list[DiscoveredFile],
    dbt_roots: set[Path],
    assets: list[Asset],
    edges: list[Edge],
    graph: dict,
    analytics: dict,
    findings: list,
    scores: dict[str, int],
    parse_warnings: list[str] | None = None,
) -> dict:
    payload = {
        "version": version,
        "scan_root": str(scan_root),
        "discovered_file_count": len(files),
        "parsed_sql_file_count": _parsed_sql_file_count(files, dbt_roots),
        "parsed_airflow_file_count": _parsed_airflow_file_count(files),
        "parsed_spark_file_count": _parsed_spark_file_count(files),
        "parsed_dbt_project_count": _parsed_dbt_project_count(sorted(dbt_roots)),
        "assets": [a.model_dump(mode="json") for a in assets],
        "edges": [e.model_dump(mode="json") for e in edges],
        "graph": graph,
        "analytics": analytics,
        "findings": [f.model_dump(mode="json") for f in findings],
        "scores": dict(scores),
    }
    if parse_warnings:
        payload["parse_warnings"] = list(parse_warnings)
    return payload


def _write_snapshot(scan_root: Path, payload: dict) -> Path | None:
    """Write daily + timestamped snapshots and prune old snapshots."""
    now = datetime.now(tz=UTC)
    day = now.strftime("%Y-%m-%d")
    stamp = now.strftime("%Y-%m-%dT%H%M%S_%fZ")
    snapshot_dir = scan_root / ".pipescope" / "snapshots"
    try:
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        body = json.dumps(payload, indent=2, ensure_ascii=False)
        daily_snap = snapshot_dir / f"{day}.json"
        daily_snap.write_text(body, encoding="utf-8")
        point_in_time = snapshot_dir / f"{stamp}.json"
        point_in_time.write_text(body, encoding="utf-8")
        _prune_snapshots(snapshot_dir, _snapshot_retention_days())
        return daily_snap
    except OSError:
        return None


def _snapshot_retention_days() -> int:
    """Retention window for snapshots (env: ``PIPESCOPE_SNAPSHOT_RETENTION_DAYS``)."""
    raw = os.getenv("PIPESCOPE_SNAPSHOT_RETENTION_DAYS", "30").strip()
    try:
        days = int(raw)
    except ValueError:
        return 30
    return max(0, days)


def _prune_snapshots(snapshot_dir: Path, retention_days: int) -> None:
    """Delete timestamped snapshots older than retention window."""
    if retention_days < 0:
        return
    cutoff = datetime.now(tz=UTC) - timedelta(days=retention_days)
    for snap in snapshot_dir.glob("*.json"):
        name = snap.name
        # Keep daily snapshots (YYYY-MM-DD.json) as latest checkpoint for each day.
        if len(name) == 15 and name[4] == "-" and name[7] == "-" and name.endswith(".json"):
            continue
        try:
            mtime = datetime.fromtimestamp(snap.stat().st_mtime, tz=UTC)
            if mtime < cutoff:
                snap.unlink()
        except OSError:
            continue


def _overall_health_score(scores: dict[str, int]) -> int:
    positive = [
        "dead_assets",
        "test_coverage",
        "documentation",
        "ownership",
        "contracts",
        "cost_hotspots",
    ]
    vals = [scores[k] for k in positive if k in scores]
    if "complexity" in scores:
        vals.append(max(0, min(100, 100 - int(scores["complexity"]))))
    if not vals:
        return 100
    return int(round(sum(vals) / len(vals)))


def _gha_annotation_prefix(severity: str) -> str:
    s = (severity or "").lower()
    if s == "critical":
        return "error"
    if s == "warning":
        return "warning"
    return "notice"


def _emit_github_annotations(findings: list) -> None:
    for f in findings:
        sev = _gha_annotation_prefix(getattr(f, "severity", "info"))
        cat = getattr(f, "category", "")
        asset = getattr(f, "asset_name", "")
        msg = getattr(f, "message", "")
        print(f"::{sev} title=PipeScope::{cat} [{asset}] {msg}")


def _github_pr_number_from_env() -> int | None:
    ref = os.getenv("GITHUB_REF", "").strip()
    m = re.match(r"^refs/pull/(\d+)/(?:merge|head)$", ref)
    if m:
        return int(m.group(1))
    return None


def _post_pr_comment_if_possible(score: int, threshold: int, findings_count: int) -> bool:
    token = os.getenv("GITHUB_TOKEN", "").strip()
    repo = os.getenv("GITHUB_REPOSITORY", "").strip()
    pr = _github_pr_number_from_env()
    if not token or not repo or not pr:
        return False
    body = {
        "body": (
            f"PipeScope CI result\n\n"
            f"- Overall score: **{score}**\n"
            f"- Threshold: **{threshold}**\n"
            f"- Findings: **{findings_count}**"
        )
    }
    req = urllib.request.Request(
        url=f"https://api.github.com/repos/{repo}/issues/{pr}/comments",
        data=json.dumps(body).encode("utf-8"),
        method="POST",
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github+json",
            "User-Agent": "pipescope-cli",
            "Content-Type": "application/json",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=8):
            return True
    except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError):
        return False


def _compute_scan_artifacts(
    *,
    root: Path,
    dialect: str | None,
    dead_asset_whitelist: str | None,
    dead_asset_terminal_tags: str | None,
    test_coverage_critical_deps: int,
    exclude: str | None = None,
    use_progress: bool = False,
    progress_console: Console | None = None,
) -> tuple[list[DiscoveredFile], list[Asset], list[Edge], dict, dict, list]:
    """Run full scan/analyzer pipeline and return core artifacts."""
    exclude_set = normalize_exclude_dir_names(exclude)
    parse_warnings: list[str] = []

    def _run_collect(progress: Progress | None) -> tuple[list, list, list, list]:
        return collect_scan(
            root,
            dialect,
            exclude_names=exclude_set,
            parse_warnings=parse_warnings,
            progress=progress,
        )

    if use_progress and progress_console is not None:
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            TextColumn("•"),
            TimeElapsedColumn(),
            console=progress_console,
        ) as progress:
            files, all_assets, all_edges, parsed_contracts = _run_collect(progress)
    else:
        files, all_assets, all_edges, parsed_contracts = _run_collect(None)

    dbt_roots = set(_collect_dbt_project_roots(root, files, exclude_set))
    pg = build_pipeline_graph(all_assets, all_edges)
    summary = graph_summary(pg.g)
    analytics = compute_scan_analytics(pg, all_assets)
    dead_analysis = analyze_dead_assets(
        pg,
        all_assets,
        whitelist=parse_dead_asset_whitelist_cli(dead_asset_whitelist),
        terminal_tag_markers=parse_dead_asset_terminal_tags_cli(dead_asset_terminal_tags),
    )
    tc_analysis = analyze_test_coverage(
        pg,
        all_assets,
        critical_downstream_threshold=test_coverage_critical_deps,
    )
    doc_analysis = analyze_documentation_coverage(all_assets)
    cx_analysis = analyze_complexity(pg, all_assets, root, dialect)
    own_analysis = analyze_ownership(all_assets, root)
    cc_analysis = analyze_contract_compliance(pg, all_assets, parsed_contracts)
    ch_analysis = analyze_cost_hotspots(pg, all_assets, root, dialect)

    analytics["dead_asset_analysis"] = dead_analysis.to_analytics_dict()
    analytics["test_coverage"] = {
        "asset_count": tc_analysis.total_count,
        "assets_with_tests": tc_analysis.tested_count,
        "coverage_ratio": tc_analysis.coverage_ratio,
    }
    analytics["test_coverage_analysis"] = tc_analysis.to_analytics_dict()
    analytics["documentation_coverage"] = {
        "asset_count": doc_analysis.total_count,
        "documented_count": doc_analysis.documented_count,
        "coverage_ratio": doc_analysis.coverage_ratio,
    }
    analytics["documentation_coverage_analysis"] = doc_analysis.to_analytics_dict()
    analytics["complexity_analysis"] = cx_analysis.to_analytics_dict()
    analytics["ownership_analysis"] = own_analysis.to_analytics_dict()
    analytics["contract_compliance_analysis"] = cc_analysis.to_analytics_dict()
    analytics["cost_hotspot_analysis"] = ch_analysis.to_analytics_dict()
    findings = (
        list(dead_analysis.findings)
        + list(tc_analysis.findings)
        + list(doc_analysis.findings)
        + list(cx_analysis.findings)
        + list(own_analysis.findings)
        + list(cc_analysis.findings)
        + list(ch_analysis.findings)
    )
    scores = {
        "dead_assets": dead_analysis.score,
        "test_coverage": tc_analysis.score,
        "documentation": doc_analysis.score,
        "complexity": cx_analysis.pipeline_score,
        "ownership": own_analysis.score,
        "contracts": cc_analysis.score,
        "cost_hotspots": ch_analysis.score,
    }
    payload_dict = _build_scan_payload_dict(
        version=__version__,
        scan_root=root,
        files=files,
        dbt_roots=dbt_roots,
        assets=all_assets,
        edges=all_edges,
        graph=summary,
        analytics=analytics,
        findings=findings,
        scores=scores,
        parse_warnings=parse_warnings,
    )
    return files, all_assets, all_edges, payload_dict, scores, findings


@app.command(
    epilog=(
        "Examples:\n"
        "  pipescope scan . --format json\n"
        "  pipescope scan ./transforms -d snowflake --exclude node_modules,.venv,venv\n"
        "  pipescope scan . --test-coverage-critical-deps 15"
    ),
)
def scan(
    path: str = typer.Argument(
        ".",
        help="Directory to scan (Git repo or folder with SQL/dbt/Airflow/Spark).",
        show_default=True,
    ),
    dialect: str | None = typer.Option(
        None,
        "--dialect",
        "-d",
        help=(
            "SQL dialect for SQLGlot (e.g. snowflake, bigquery, postgres, duckdb). "
            "Omit for generic parsing."
        ),
    ),
    format_: str = typer.Option(
        "terminal",
        "--format",
        "-f",
        help="terminal: Rich report + HTML path; json: one JSON object on stdout.",
    ),
    exclude: str | None = typer.Option(
        None,
        "--exclude",
        "-e",
        help=(
            "Comma-separated directory names to prune while walking (e.g. "
            "node_modules,venv,.venv,.git). Case-insensitive; hidden dirs are always skipped."
        ),
    ),
    dead_asset_whitelist: str | None = typer.Option(
        None,
        "--dead-asset-whitelist",
        help="Comma-separated asset names excluded from dead-asset analysis.",
    ),
    dead_asset_terminal_tags: str | None = typer.Option(
        None,
        "--dead-asset-terminal-tags",
        help=(
            "Comma-separated substrings for intentional terminal sinks "
            "(tag keys/values; default: exposure,dashboard,export). "
            "Pass empty string to disable tag-based exclusion."
        ),
    ),
    test_coverage_critical_deps: int = typer.Option(
        10,
        "--test-coverage-critical-deps",
        help=(
            "Emit CRITICAL missing-test findings when downstream dependents exceed this "
            "(non-staging models only; default: 10)."
        ),
    ),
) -> None:
    """Scan a directory and analyze data pipeline health (lineage, tests, docs, contracts)."""
    fmt = format_.strip().lower()
    if fmt not in ("terminal", "json"):
        raise typer.BadParameter("format must be 'terminal' or 'json'")
    if test_coverage_critical_deps < 0:
        raise typer.BadParameter("--test-coverage-critical-deps must be >= 0")

    root = Path(path).resolve()
    exclude_set = normalize_exclude_dir_names(exclude)
    term = fmt == "terminal"
    console = _make_console()
    files, all_assets, all_edges, payload_dict, scores, findings = _compute_scan_artifacts(
        root=root,
        dialect=dialect,
        dead_asset_whitelist=dead_asset_whitelist,
        dead_asset_terminal_tags=dead_asset_terminal_tags,
        test_coverage_critical_deps=test_coverage_critical_deps,
        exclude=exclude,
        use_progress=term,
        progress_console=console if term else None,
    )
    summary = payload_dict["graph"]
    analytics = payload_dict["analytics"]
    dbt_roots = set(_collect_dbt_project_roots(root, files, exclude_set))
    _write_snapshot(root, payload_dict)

    if fmt == "json":
        _ensure_utf8_stdio()
        pw = payload_dict.get("parse_warnings")
        payload = format_scan_json(
            version=__version__,
            scan_root=str(root),
            discovered_file_count=len(files),
            parsed_sql_file_count=_parsed_sql_file_count(files, dbt_roots),
            parsed_airflow_file_count=_parsed_airflow_file_count(files),
            parsed_spark_file_count=_parsed_spark_file_count(files),
            parsed_dbt_project_count=_parsed_dbt_project_count(sorted(dbt_roots)),
            assets=all_assets,
            edges=all_edges,
            graph=summary,
            analytics=analytics,
            findings=findings,
            scores=scores,
            parse_warnings=pw if isinstance(pw, list) else None,
        )
        sys.stdout.write(payload)
        if not payload.endswith("\n"):
            sys.stdout.write("\n")
        return

    _ensure_utf8_stdio()
    html_report_path = Path(tempfile.gettempdir()) / "pipescope-report.html"
    write_report(
        html_report_path,
        payload_dict,
    )
    parse_warnings = payload_dict.get("parse_warnings") or []
    print_terminal_report(
        console,
        scan_root=str(root),
        discovered_file_count=len(files),
        assets=all_assets,
        edges=all_edges,
        summary=summary,
        analytics=analytics,
        scores=scores,
        findings=findings,
        html_report_path=str(html_report_path),
        parse_warnings=parse_warnings if isinstance(parse_warnings, list) else [],
    )


@app.command(
    epilog=(
        "Examples:\n"
        "  pipescope diff HEAD~1 --path .\n"
        "  pipescope diff main --exclude node_modules,venv"
    ),
)
def diff(
    ref: str = typer.Argument(
        "HEAD~1",
        help="Git revision to compare against (e.g. HEAD~1, main, abc1234).",
        show_default=True,
    ),
    path: str = typer.Option(
        ".",
        "--path",
        help="Git repository root (must be inside a work tree).",
        show_default=True,
    ),
    dialect: str | None = typer.Option(
        None,
        "--dialect",
        "-d",
        help="SQL dialect passed through to SQL parsing (same as scan).",
    ),
    exclude: str | None = typer.Option(
        None,
        "--exclude",
        "-e",
        help="Comma-separated directory names to skip while walking (same as scan).",
    ),
) -> None:
    """Compare PipeScope assets and findings: current tree vs a git ref (via worktree)."""
    root = Path(path).resolve()
    _ensure_utf8_stdio()
    console = _make_console()

    # Validate git repository first.
    chk = subprocess.run(
        ["git", "-C", str(root), "rev-parse", "--is-inside-work-tree"],
        capture_output=True,
        text=True,
    )
    if chk.returncode != 0:
        raise typer.BadParameter("diff requires a git repository at --path")

    console.print(f"[bold]Comparing current workspace vs {ref}[/]")
    (
        _cur_files,
        cur_assets,
        _cur_edges,
        _cur_payload,
        _cur_scores,
        cur_findings,
    ) = _compute_scan_artifacts(
        root=root,
        dialect=dialect,
        dead_asset_whitelist=None,
        dead_asset_terminal_tags=None,
        test_coverage_critical_deps=10,
        exclude=exclude,
        use_progress=False,
        progress_console=None,
    )

    with tempfile.TemporaryDirectory(prefix="pipescope-diff-") as tmp:
        worktree = Path(tmp) / "repo"
        add_cmd = [
            "git",
            "-C",
            str(root),
            "worktree",
            "add",
            "--detach",
            str(worktree),
            ref,
        ]
        add = subprocess.run(add_cmd, capture_output=True, text=True)
        if add.returncode != 0:
            raise typer.BadParameter(f"failed to checkout ref {ref}: {add.stderr.strip()}")

        try:
            _prev_files, prev_assets, _prev_edges, _prev_payload, _prev_scores, prev_findings = (
                _compute_scan_artifacts(
                    root=worktree,
                    dialect=dialect,
                    dead_asset_whitelist=None,
                    dead_asset_terminal_tags=None,
                    test_coverage_critical_deps=10,
                    exclude=exclude,
                    use_progress=False,
                    progress_console=None,
                )
            )
        finally:
            subprocess.run(
                ["git", "-C", str(root), "worktree", "remove", "--force", str(worktree)],
                capture_output=True,
                text=True,
            )

    cur_asset_names = {a.name for a in cur_assets}
    prev_asset_names = {a.name for a in prev_assets}
    new_assets = sorted(cur_asset_names - prev_asset_names)
    removed_assets = sorted(prev_asset_names - cur_asset_names)

    def _fkey(f: object) -> tuple:
        return (
            getattr(f, "severity", ""),
            getattr(f, "category", ""),
            getattr(f, "asset_name", ""),
            getattr(f, "message", ""),
        )

    cur_f = {_fkey(f) for f in cur_findings}
    prev_f = {_fkey(f) for f in prev_findings}
    new_findings = sorted(cur_f - prev_f)
    resolved_findings = sorted(prev_f - cur_f)

    summary_tbl = Table(title="Diff summary", show_header=True, header_style="bold")
    summary_tbl.add_column("Metric", style="cyan")
    summary_tbl.add_column("Count", justify="right")
    summary_tbl.add_row("New assets", str(len(new_assets)))
    summary_tbl.add_row("Removed assets", str(len(removed_assets)))
    summary_tbl.add_row("New findings", str(len(new_findings)))
    summary_tbl.add_row("Resolved findings", str(len(resolved_findings)))
    console.print(summary_tbl)

    def _print_list(title: str, rows: list[str], color: str) -> None:
        t = Table(title=title, show_header=True, header_style="bold")
        t.add_column("Item", style=color, overflow="fold")
        for r in rows[:30]:
            t.add_row(r)
        if len(rows) > 30:
            t.add_row(f"... +{len(rows)-30} more")
        console.print(t)

    _print_list("New assets", new_assets, "green")
    _print_list("Removed assets", removed_assets, "red")

    nf = Table(title="New findings", show_header=True, header_style="bold")
    nf.add_column("Severity", style="yellow")
    nf.add_column("Category", style="cyan")
    nf.add_column("Asset")
    nf.add_column("Message", overflow="fold")
    for sev, cat, asset, msg in new_findings[:30]:
        nf.add_row(str(sev), str(cat), str(asset), str(msg))
    if len(new_findings) > 30:
        nf.add_row("...", "...", "...", f"+{len(new_findings)-30} more")
    console.print(nf)

    rf = Table(title="Resolved findings", show_header=True, header_style="bold")
    rf.add_column("Severity", style="blue")
    rf.add_column("Category", style="cyan")
    rf.add_column("Asset")
    rf.add_column("Message", overflow="fold")
    for sev, cat, asset, msg in resolved_findings[:30]:
        rf.add_row(str(sev), str(cat), str(asset), str(msg))
    if len(resolved_findings) > 30:
        rf.add_row("...", "...", "...", f"+{len(resolved_findings)-30} more")
    console.print(rf)
    console.print(
        Panel(
            f"[bold]Compared[/] current vs [bold]{ref}[/] at {root}",
            border_style="dim",
        )
    )


@app.command(
    epilog=(
        "Examples:\n"
        "  pipescope ci --threshold 70\n"
        "  pipescope ci --path ./dbt --exclude .venv,venv --threshold 80"
    ),
)
def ci(
    threshold: int = typer.Option(
        70,
        "--threshold",
        help="Exit with code 1 when blended overall score is below this (0–100).",
    ),
    path: str = typer.Option(
        ".",
        "--path",
        help="Directory to scan (repository root recommended).",
        show_default=True,
    ),
    dialect: str | None = typer.Option(
        None,
        "--dialect",
        "-d",
        help="SQL dialect for parsing (same as scan).",
    ),
    exclude: str | None = typer.Option(
        None,
        "--exclude",
        "-e",
        help="Comma-separated directory names to skip while walking (same as scan).",
    ),
) -> None:
    """Run a full scan for CI: GitHub Actions annotations, optional PR comment, threshold gate."""
    if threshold < 0 or threshold > 100:
        raise typer.BadParameter("--threshold must be between 0 and 100")
    root = Path(path).resolve()
    _files, _assets, _edges, payload, scores, findings = _compute_scan_artifacts(
        root=root,
        dialect=dialect,
        dead_asset_whitelist=None,
        dead_asset_terminal_tags=None,
        test_coverage_critical_deps=10,
        exclude=exclude,
        use_progress=False,
        progress_console=None,
    )
    _write_snapshot(root, payload)
    overall = _overall_health_score(scores)

    _ensure_utf8_stdio()
    print(f"PipeScope overall score: {overall} (threshold: {threshold})")
    _emit_github_annotations(findings)
    posted = _post_pr_comment_if_possible(overall, threshold, len(findings))
    if posted:
        print("Posted PipeScope score comment to PR.")

    if overall < threshold:
        raise typer.Exit(code=1)


if __name__ == "__main__":
    app()
