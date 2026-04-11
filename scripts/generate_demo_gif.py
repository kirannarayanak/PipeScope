#!/usr/bin/env python3
"""Build docs/demo/lineagescope-demo.gif (Pillow) for README embedding.

Prefer a real recording via asciinema + agg (see docs/demo/README.md). This
script produces a terminal-styled GIF when agg is not available or for CI
regeneration: ``python scripts/generate_demo_gif.py``
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OUT_GIF = ROOT / "docs" / "demo" / "lineagescope-demo.gif"

LINES = [
    "$ lineagescope scan . --format terminal",
    "",
    "Walking repository ... done",
    "dbt packages .......... done",
    "Parsing source files .. done",
    "Data contracts ........ done",
    "",
    "Found 37 data files",
    "Overall health score 78/100",
    "HTML report: /tmp/lineagescope-report.html",
]


def _pick_mono_font(size: int):
    try:
        from PIL import ImageFont
    except ImportError as e:
        raise SystemExit("Pillow is required: pip install Pillow") from e

    candidates = [
        Path(r"C:\Windows\Fonts\consola.ttf"),
        Path(r"C:\Windows\Fonts\cour.ttf"),
        Path("/usr/share/fonts/truetype/dejavu/DejaVuSansMono.ttf"),
        Path("/Library/Fonts/Menlo.ttc"),
    ]
    for p in candidates:
        if p.is_file():
            try:
                return ImageFont.truetype(str(p), size=size)
            except OSError:
                continue
    return ImageFont.load_default()


def main() -> int:
    from PIL import Image, ImageDraw

    w, h = 920, 420
    pad = 36
    line_h = 34
    font = _pick_mono_font(22)

    bg = (18, 18, 22)
    fg = (180, 255, 200)
    prompt = (120, 200, 255)
    accent = (220, 160, 255)

    frames: list[Image.Image] = []
    durations: list[int] = []

    visible = 0
    while visible <= len(LINES):
        img = Image.new("RGB", (w, h), bg)
        draw = ImageDraw.Draw(img)
        draw.rounded_rectangle((12, 12, w - 12, h - 12), radius=14, outline=(55, 55, 70), width=2)
        y = pad
        for line in LINES[:visible]:
            color = prompt if line.startswith("$") else fg
            if "health score" in line:
                color = accent
            draw.text((pad, y), line, font=font, fill=color)
            y += line_h
        frames.append(img)
        durations.append(450 if visible < len(LINES) else 2200)
        visible += 1

    OUT_GIF.parent.mkdir(parents=True, exist_ok=True)
    frames[0].save(
        OUT_GIF,
        save_all=True,
        append_images=frames[1:],
        duration=durations,
        loop=0,
        optimize=True,
    )
    print(f"Wrote {OUT_GIF}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
