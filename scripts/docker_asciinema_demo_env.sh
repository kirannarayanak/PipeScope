#!/usr/bin/env bash
# Prepare Python 3.12 + asciinema v3 + lineagescope inside a Linux container (see docs/demo/README.md).
set -eu
export DEBIAN_FRONTEND=noninteractive

apt-get update
apt-get install -y --no-install-recommends ca-certificates curl git

ASC_V="${ASCIINEMA_VERSION:-v3.2.0}"
# musl binary runs on Bookworm; gnu binary requires GLIBC_2.39+.
BIN="asciinema-x86_64-unknown-linux-musl"
curl -fsSL -o /usr/local/bin/asciinema \
  "https://github.com/asciinema/asciinema/releases/download/${ASC_V}/${BIN}"
chmod +x /usr/local/bin/asciinema

pip install --no-cache-dir -e /data

echo ""
echo "Ready: asciinema $(asciinema --version 2>&1 | head -n1)"
echo "        lineagescope $(lineagescope --version 2>&1 | head -n1 || true)"
echo "Example: cd /data/docs/demo && asciinema rec lineagescope-demo.cast"
echo ""

exec bash
