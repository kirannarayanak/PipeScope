# Terminal demo (asciinema + agg)

## Bundled assets

| File | Purpose |
| --- | --- |
| `pipescope-demo.cast` | Sample [asciinema v2](https://docs.asciinema.org/manual/asciicast/v2/) recording (hand-authored). |
| `pipescope-demo.gif` | Looping preview for the root README (regenerate below). |

## Regenerate the GIF (no agg required)

From the repo root (install Pillow in your env if needed: `pip install Pillow`):

```bash
python scripts/generate_demo_gif.py
```

## High-fidelity capture (asciinema + agg)

1. Install [asciinema](https://asciinema.org/docs/installation) and [agg](https://github.com/asciinema/agg#installation).

2. Record:

   ```bash
   asciinema rec pipescope-demo.cast
   ```

   Run a real scan, then `exit`.

3. Convert:

   ```bash
   agg --font-size 20 pipescope-demo.cast pipescope-demo.gif
   ```

4. Commit the updated `pipescope-demo.gif` and optionally replace `pipescope-demo.cast`.
