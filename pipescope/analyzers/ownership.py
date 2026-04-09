"""Ownership mapping: CODEOWNERS, dbt ``meta.owner``, git last-commit author."""

from __future__ import annotations

import fnmatch
import subprocess
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from pipescope.models import Asset, Finding, Severity

# ~6 months for stale detection
STALE_DELTA = timedelta(days=183)


def _scope_assets(assets: list[Asset]) -> list[Asset]:
    return [a for a in assets if a.tags.get("role") != "query_block"]


def _git_toplevel(start: Path) -> Path | None:
    try:
        cp = subprocess.run(
            ["git", "rev-parse", "--show-toplevel"],
            cwd=str(start),
            capture_output=True,
            text=True,
            timeout=8,
        )
        if cp.returncode != 0:
            return None
        p = Path(cp.stdout.strip()).resolve()
        return p if p.is_dir() else None
    except (OSError, subprocess.TimeoutExpired):
        return None


def _find_codeowners_file(
    scan_root: Path,
    git_root: Path | None,
) -> tuple[Path | None, Path | None]:
    """Return (file, root_that_patterns_are_relative_to), first match in search order."""
    ordered_roots: list[Path] = []
    if git_root is not None:
        ordered_roots.append(git_root.resolve())
    sr = scan_root.resolve()
    if sr not in ordered_roots:
        ordered_roots.append(sr)
    for root in ordered_roots:
        for candidate in (root / ".github" / "CODEOWNERS", root / "CODEOWNERS"):
            if candidate.is_file():
                return candidate, root
    return None, None


def _parse_codeowners_line(line: str) -> tuple[str, list[str]] | None:
    raw = line.strip()
    if not raw or raw.startswith("#"):
        return None
    # Strip inline comments (unquoted #)
    if "#" in raw:
        before, _sep, _after = raw.partition("#")
        raw = before.rstrip()
        if not raw:
            return None
    parts = raw.split()
    if len(parts) < 2:
        return None
    pattern, owners = parts[0], parts[1:]
    owners = [o for o in owners if o]
    if not owners:
        return None
    return pattern, owners


def parse_codeowners_text(text: str) -> list[tuple[str, list[str]]]:
    """Parse CODEOWNERS body into (pattern, owners) in file order (last match wins)."""
    out: list[tuple[str, list[str]]] = []
    for line in text.splitlines():
        parsed = _parse_codeowners_line(line)
        if parsed:
            out.append(parsed)
    return out


def codeowners_pattern_matches(pattern: str, rel_path: str) -> bool:
    """GitHub-style CODEOWNERS glob: ``*`` / ``?`` per segment; ``**``; basename-only patterns."""
    pat = pattern.replace("\\", "/").strip()
    if not pat or pat.startswith("#"):
        return False
    if pat.startswith("/"):
        pat = pat[1:]
    path = rel_path.replace("\\", "/").strip("/")

    if "/" not in pat:
        name = path.split("/")[-1] if path else ""
        return bool(name) and fnmatch.fnmatch(name, pat)

    pat_parts = pat.split("/")
    path_parts = path.split("/") if path else []

    def match_segments(pi: int, pti: int) -> bool:
        if pi >= len(pat_parts):
            return pti >= len(path_parts)
        if pat_parts[pi] == "**":
            if pi == len(pat_parts) - 1:
                return True
            for j in range(pti, len(path_parts) + 1):
                if match_segments(pi + 1, j):
                    return True
            return False
        if pti >= len(path_parts):
            return False
        if not fnmatch.fnmatch(path_parts[pti], pat_parts[pi]):
            return False
        return match_segments(pi + 1, pti + 1)

    return match_segments(0, 0)


def _codeowners_owner_for_path(
    entries: list[tuple[str, list[str]]],
    rel_path: str,
) -> str | None:
    """Last matching pattern wins (GitHub). Owners joined with comma."""
    matched: list[str] | None = None
    for pattern, owners in entries:
        if codeowners_pattern_matches(pattern, rel_path):
            matched = owners
    if not matched:
        return None
    return ", ".join(matched)


def _path_relative_to_root(asset: Asset, scan_root: Path, base: Path) -> str | None:
    try:
        if Path(asset.file_path).is_absolute():
            full = Path(asset.file_path).resolve()
        else:
            full = (scan_root / asset.file_path).resolve()
        rel = full.relative_to(base.resolve())
    except ValueError:
        return None
    return str(rel).replace("\\", "/")


def _git_last_commit_info(repo_root: Path, rel_file: str) -> tuple[str | None, datetime | None]:
    """Return (author display string, commit time UTC) for ``rel_file`` relative to repo."""
    rel_file = rel_file.replace("\\", "/")
    try:
        cp = subprocess.run(
            [
                "git",
                "-C",
                str(repo_root),
                "log",
                "-1",
                "--format=%ct|%an",
                "--",
                rel_file,
            ],
            capture_output=True,
            text=True,
            timeout=12,
        )
        if cp.returncode != 0:
            return None, None
        line = cp.stdout.strip().splitlines()
        if not line:
            return None, None
        first = line[0]
        if "|" not in first:
            return None, None
        ts_s, author = first.split("|", 1)
        ts = int(ts_s.strip())
        dt = datetime.fromtimestamp(ts, tz=UTC)
        auth = author.strip() or None
        return auth, dt
    except (OSError, subprocess.TimeoutExpired, ValueError):
        return None, None


@dataclass
class OwnershipAnalysisResult:
    findings: list[Finding] = field(default_factory=list)
    score: int = 100
    assets_with_owner: int = 0
    total_count: int = 0
    coverage_ratio: float | None = None
    stale_count: int = 0
    no_owner_count: int = 0

    def to_analytics_dict(self) -> dict[str, Any]:
        return {
            "ownership_score": self.score,
            "assets_with_owner": self.assets_with_owner,
            "total_count": self.total_count,
            "coverage_ratio": self.coverage_ratio,
            "stale_count": self.stale_count,
            "no_owner_count": self.no_owner_count,
        }


def analyze_ownership(assets: list[Asset], scan_root: Path) -> OwnershipAnalysisResult:
    """Resolve ownership: CODEOWNERS → dbt ``meta.owner`` → git author; flag gaps and staleness."""
    scope = _scope_assets(assets)
    scan_root = scan_root.resolve()
    git_root = _git_toplevel(scan_root)
    co_path, co_root = _find_codeowners_file(scan_root, git_root)
    entries: list[tuple[str, list[str]]] = []
    if co_path is not None and co_root is not None:
        try:
            text = co_path.read_text(encoding="utf-8", errors="ignore")
            entries = parse_codeowners_text(text)
        except OSError:
            entries = []

    now = datetime.now(tz=UTC)
    stale_cutoff = now - STALE_DELTA

    findings: list[Finding] = []
    with_owner = 0
    stale_n = 0
    no_owner_n = 0

    # Cache git lookups per file under a repo root
    git_cache: dict[tuple[str, str], tuple[str | None, datetime | None]] = {}

    if not scope:
        return OwnershipAnalysisResult(
            score=100,
            total_count=0,
            coverage_ratio=None,
        )

    for asset in scope:
        owner: str | None = None
        rel_for_co: str | None = None
        if co_root is not None:
            rel_for_co = _path_relative_to_root(asset, scan_root, co_root)
            if rel_for_co and entries:
                owner = _codeowners_owner_for_path(entries, rel_for_co)

        if owner is None and asset.owner:
            owner = asset.owner.strip() or None

        rel_for_git: str | None = None
        git_author: str | None = None
        git_dt: datetime | None = None
        if git_root is not None:
            rel_for_git = _path_relative_to_root(asset, scan_root, git_root)
            if rel_for_git:
                key = (str(git_root), rel_for_git)
                if key in git_cache:
                    git_author, git_dt = git_cache[key]
                else:
                    git_author, git_dt = _git_last_commit_info(git_root, rel_for_git)
                    git_cache[key] = (git_author, git_dt)

        if owner is None and git_author:
            owner = git_author

        if owner:
            with_owner += 1
        else:
            no_owner_n += 1
            findings.append(
                Finding(
                    severity=Severity.WARNING,
                    category="no_owner",
                    asset_name=asset.name,
                    message=(
                        "No identifiable owner: add CODEOWNERS, dbt meta.owner, "
                        "or commit history"
                    ),
                    file_path=asset.file_path or None,
                )
            )

        if git_dt is not None and git_dt < stale_cutoff:
            stale_n += 1
            findings.append(
                Finding(
                    severity=Severity.WARNING,
                    category="stale_asset",
                    asset_name=asset.name,
                    message=(
                        f"Last commit on file is older than ~6 months "
                        f"({git_dt.date().isoformat()})"
                    ),
                    file_path=asset.file_path or None,
                )
            )

    total = len(scope)
    ratio = with_owner / total if total else None
    score = max(0, min(100, int(round(ratio * 100)))) if ratio is not None else 100

    return OwnershipAnalysisResult(
        findings=findings,
        score=score,
        assets_with_owner=with_owner,
        total_count=total,
        coverage_ratio=round(ratio, 6) if ratio is not None else None,
        stale_count=stale_n,
        no_owner_count=no_owner_n,
    )


def analyze() -> list[Finding]:
    """Stub without scan context; use :func:`analyze_ownership`."""
    return []
