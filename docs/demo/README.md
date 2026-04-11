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

## Record on Windows (Docker), step by step

LineageScope needs **Python ≥ 3.11**, so use Docker image **`python:3.12-bookworm`** (not `ghcr.io/asciinema/asciinema`, which is older Python).

### Before you start

1. **Docker Desktop** is installed and running (whale icon in the tray).
2. You are in the **LineageScope repo folder** (the one that contains `scripts\` and `docs\`).

### Step 1 — Open PowerShell in the repo

Press **Win**, type **PowerShell**, open **Windows PowerShell**. Then:

```powershell
cd C:\Users\HP\Desktop\lineagescope
```

(Change the path if your clone lives somewhere else.)

### Step 2 — Start the recording container (recommended)

This installs **asciinema** and **lineagescope** inside Linux and drops you into **bash**. You do **not** need `pwsh` (PowerShell 7); built-in **Windows PowerShell** is fine:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\enter-docker-demo-env.ps1
```

Wait until you see a prompt like `root@xxxxxxxxxxxx:/data#`. The first run may take a minute (image + `apt` + `pip`). The script installs **LineageScope from your mounted repo** (`pip install -e /data`), not only the PyPI wheel, so CLI changes on your branch (for example `--version`) match what you run in the container.

If Windows asks about **execution policy**, the `-ExecutionPolicy Bypass` flag above is enough for this one command.

### Step 3 — Check tools (inside the container)

Type these one at a time (or paste with **right‑click** or **Ctrl+Shift+V**):

```bash
asciinema --version
lineagescope --version
```

If you see **`GLIBC_2.39' not found`** for asciinema, your setup script is outdated: it must install the **musl** binary (`asciinema-x86_64-unknown-linux-musl`), not the gnu one. Pull the latest `scripts/enter-docker-demo-env.ps1` from the repo.

You should see version text for both. If `asciinema` says **command not found**, stop and say so (we can fix the download step).

### Step 4 — Record the demo

```bash
cd /data/docs/demo
asciinema rec lineagescope-demo.cast
```

asciinema will tell you how to **finish** the recording (often **Ctrl+D** or type **`exit`**). Then run a short demo, for example:

```bash
lineagescope --help
lineagescope scan /data/tests/fixtures --dialect postgres
```

When done, end the recording as asciinema instructed.

### Step 5 — Leave the container

```bash
exit
```

Your new **`lineagescope-demo.cast`** file is on Windows under **`docs\demo\`** (the repo is mounted as `/data`).

### Step 6 — (Optional) Make a GIF on Windows

Install **agg** (e.g. `winget install asciinema.agg`), then in **PowerShell**:

```powershell
cd C:\Users\HP\Desktop\lineagescope\docs\demo
agg --font-size 20 lineagescope-demo.cast lineagescope-demo.gif
```

### Alternative: Docker + `.sh` on the mount

Only if the script file is saved with **LF** line endings:

```powershell
docker run --rm -it -v "${PWD}:/data" -w /data python:3.12-bookworm bash /data/scripts/docker_asciinema_demo_env.sh
```

Use **`${PWD}`** (with braces), not `$PWD:` — otherwise PowerShell errors.

## Stylized GIF without agg

From the repo root (requires Pillow / `pip install -e ".[dev]"`):

```bash
python scripts/generate_demo_gif.py
```

Writes `docs/demo/lineagescope-demo.gif`.
