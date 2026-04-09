"""Analysis modules (dead assets, coverage, complexity, etc.)."""

from pipescope.analyzers.complexity import analyze_complexity
from pipescope.analyzers.dead_assets import (
    analyze_dead_assets,
    parse_dead_asset_terminal_tags_cli,
    parse_dead_asset_whitelist_cli,
)
from pipescope.analyzers.doc_coverage import analyze_documentation_coverage
from pipescope.analyzers.test_coverage import (
    DEFAULT_CRITICAL_DOWNSTREAM_THRESHOLD,
    analyze_test_coverage,
)

__all__ = [
    "DEFAULT_CRITICAL_DOWNSTREAM_THRESHOLD",
    "analyze_complexity",
    "analyze_dead_assets",
    "analyze_documentation_coverage",
    "analyze_test_coverage",
    "parse_dead_asset_terminal_tags_cli",
    "parse_dead_asset_whitelist_cli",
]
