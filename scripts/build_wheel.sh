#!/usr/bin/env bash
# Build the rithmic-nt-connect wheel with the rithmic-gateway binary bundled.
#
# The wheel is already per-platform (it contains the PyO3 `_lib.so`), so we
# drop the matching cargo-built `rithmic-gateway` binary into the wheel data
# dir. Consumers get the adapter, the rithmic_gateway client, and the native
# gateway binary from a single `pip install` — no RITHMIC_GATEWAY_BIN, no
# `target/` on their disk, no separate cargo build.
#
# Usage:
#   scripts/build_wheel.sh [--install]
#   --install  additionally `pip install` the built wheel into the current env
set -euo pipefail

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_dir"

install=0
for arg in "$@"; do
  case "$arg" in
    --install) install=1 ;;
    *) echo "unknown arg: $arg" >&2; exit 2 ;;
  esac
done

if ! command -v maturin >/dev/null 2>&1; then
  echo "maturin not found; run 'uv sync --extra dev' or 'pip install maturin'" >&2
  exit 1
fi

echo "==> cargo build -p rithmic-gateway --release"
cargo build -p rithmic-gateway --release

# Honor CARGO_TARGET_DIR (resolve_gateway_bin also respects it).
cargo_target_dir="${CARGO_TARGET_DIR:-target}"
bin_path="$cargo_target_dir/release/rithmic-gateway"
if [ ! -x "$bin_path" ]; then
  echo "gateway binary not found: $bin_path" >&2
  exit 1
fi

# Bundle the rithmic_gateway pure-Python client AND the native binary into the
# wheel data dir under purelib, so both install as
# <site-packages>/rithmic_gateway/... . maturin's python-source only packages
# the module matching the lib name (rithmic_nt_connect), so the shared gateway
# client package must be carried here alongside the binary it auto-spawns.
# The committed marker wheel-data/purelib/.gitkeep is preserved so maturin's
# data dir always exists (non-script builds/CI do not hard-fail on its absence).
dest="wheel-data/purelib/rithmic_gateway"
rm -rf "$dest"
mkdir -p "$dest/bin"
cp -R python/rithmic_gateway/*.py "$dest/"
cp -R python/rithmic_gateway/v1 "$dest/v1"
cp "$bin_path" "$dest/bin/rithmic-gateway"
find "$dest" -name '__pycache__' -type d -prune -exec rm -rf {} +

echo "==> maturin build"
maturin build --release
if [ "$install" -eq 1 ]; then
  wheel="$(ls -t "$cargo_target_dir/wheels/rithmic_nt_connect-*.whl" | head -1)"
  echo "==> pip install $wheel"
  pip install --force-reinstall "$wheel"
fi
