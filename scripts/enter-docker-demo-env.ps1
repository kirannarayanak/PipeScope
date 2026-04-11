# LineageScope demo recording: Python 3.12 + asciinema + lineagescope in Docker (no .sh CRLF issues).
# From repo root (Windows PowerShell 5 or 7):
#   powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\enter-docker-demo-env.ps1
# If you have PowerShell 7 installed:  pwsh -File .\scripts\enter-docker-demo-env.ps1
$ErrorActionPreference = "Stop"
$mount = (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot "..")).Path
# Use musl-linked asciinema: the "gnu" binary needs GLIBC_2.39+; python:3.12-bookworm (Debian 12) has older glibc.
# Install LineageScope editable from the mounted repo (matches your branch; PyPI wheel may lag behind CLI flags like --version).
docker run --rm -it -v "${mount}:/data" -w /data python:3.12-bookworm bash -c "apt-get update && apt-get install -y --no-install-recommends ca-certificates curl git && curl -fsSL -o /usr/local/bin/asciinema https://github.com/asciinema/asciinema/releases/download/v3.2.0/asciinema-x86_64-unknown-linux-musl && chmod +x /usr/local/bin/asciinema && pip install --no-cache-dir -e /data && exec bash"
