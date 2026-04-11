# Demo assets

| File | Role |
| --- | --- |
| `lineagescope-demo.cast` | Sample [asciinema v2](https://docs.asciinema.org/manual/asciicast/v2/) recording (hand-authored). |
| `lineagescope-demo.gif` | Looping preview for the root README (regenerate below). |

## Regenerate the GIF (pixel-perfect)

1. Install [agg](https://github.com/asciinema/agg) (or use another cast→GIF workflow).
2. Record a real session (optional):

   ```bash
   cd docs/demo
   asciinema rec lineagescope-demo.cast
   ```

3. Convert:

   ```bash
   agg --font-size 20 lineagescope-demo.cast lineagescope-demo.gif
   ```

4. Commit the updated `lineagescope-demo.gif` and optionally replace `lineagescope-demo.cast`.

## Stylized GIF without agg

From the repo root (requires Pillow / `pip install -e ".[dev]"`):

```bash
python scripts/generate_demo_gif.py
```

Writes `docs/demo/lineagescope-demo.gif`.
