#!/usr/bin/env bash
# Build everything and run the whole benchmark matrix on a Linux machine.
#
#   git clone -b reuseport-workers https://github.com/artech-git/compio-pool
#   cd compio-pool
#   nohup crates/deadpool-baseline/run-on-vm.sh > bench.log 2>&1 &     # survives a dropped ssh
#   tail -f bench.log
#
# The repository's default branch is `experimental`, which holds an older design with none of
# the examples this needs, so a plain `git clone` is the wrong checkout. Extra arguments go to
# matrix.py: `--dry-run` to see the plan and the time, `--quick` for a few-minute smoke test,
# `--only scale512,payload` for part of the matrix, `--help` for the rest.
#
# Needs: Linux with io_uring enabled (/proc/sys/kernel/io_uring_disabled = 0), python3, a C
# linker (build-essential), and either Rust >= 1.95 or network access to install it with
# rustup into $HOME (no sudo). Nothing is installed system-wide and no kernel setting is
# changed.
set -euo pipefail
cd "$(dirname "$0")/../.."

echo "checkout: $PWD  branch: $(git rev-parse --abbrev-ref HEAD 2>/dev/null || echo '?')  commit: $(git rev-parse --short HEAD 2>/dev/null || echo '?')"
for f in examples/echo.rs examples/load.rs src/worker.rs crates/deadpool-baseline/matrix.py; do
    [ -f "$f" ] || {
        echo "missing $f: this is not the reuseport-workers checkout (the default branch, experimental, is an older design)." >&2
        echo "  git clone -b reuseport-workers https://github.com/artech-git/compio-pool" >&2
        exit 1
    }
done

if [ -r /proc/sys/kernel/io_uring_disabled ] && [ "$(cat /proc/sys/kernel/io_uring_disabled)" != "0" ]; then
    echo "io_uring_disabled=$(cat /proc/sys/kernel/io_uring_disabled): compio-pool cannot run here (see Documentation/admin-guide/sysctl/kernel.rst)." >&2
    exit 1
fi
command -v python3 >/dev/null || { echo "python3 is required" >&2; exit 1; }
command -v cc >/dev/null || { echo "no C linker found: install build-essential (Debian/Ubuntu) or your distro's equivalent" >&2; exit 1; }

# Rust >= 1.95 (compio's executor needs cfg_select!).
need="1.95"
if [ -f "$HOME/.cargo/env" ]; then . "$HOME/.cargo/env"; fi
have="$(rustc --version 2>/dev/null | awk '{print $2}' || true)"
if [ -z "$have" ] || [ "$(printf '%s\n%s\n' "$need" "$have" | sort -V | head -n1)" != "$need" ]; then
    if command -v rustup >/dev/null; then
        echo "updating Rust ($have -> stable)"
        rustup update stable
    else
        echo "installing Rust with rustup into \$HOME"
        curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal
    fi
    . "$HOME/.cargo/env"
fi
echo "rustc: $(rustc --version)"

cargo build --release --locked --example echo --example load
cargo build --release --locked --manifest-path crates/deadpool-baseline/Cargo.toml \
    --example echo --example echo_tpc

exec python3 crates/deadpool-baseline/matrix.py "$@"
