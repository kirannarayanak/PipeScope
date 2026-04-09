"""Analysis modules (dead assets, coverage, complexity, etc.)."""

from pipescope.analyzers.dead_assets import (
    analyze_dead_assets,
    parse_dead_asset_terminal_tags_cli,
    parse_dead_asset_whitelist_cli,
)

__all__ = [
    "analyze_dead_assets",
    "parse_dead_asset_terminal_tags_cli",
    "parse_dead_asset_whitelist_cli",
]
