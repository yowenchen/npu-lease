#!/bin/sh
set -eu

if [ "$#" -gt 1 ]; then
    printf '%s\n' "Usage: $0 [absolute-prefix]" >&2
    exit 2
fi

prefix=${1:-/usr/local}
case "$prefix" in
    /*) ;;
    *) printf '%s\n' "Installation prefix must be an absolute path." >&2; exit 2 ;;
esac

if ! /usr/bin/python3 -c 'import sys; sys.exit(sys.version_info < (3, 11))'; then
    printf '%s\n' "npu-lease requires /usr/bin/python3 version 3.11 or later." >&2
    exit 1
fi

source_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
bin_dir="$prefix/bin"
if [ ! -d "$bin_dir" ]; then
    install -d -m 0755 -- "$bin_dir"
fi
staged=$(mktemp "$bin_dir/.npu-lease.XXXXXX")
trap 'unlink "$staged" 2>/dev/null || :' EXIT
trap 'exit 1' HUP INT TERM
/usr/bin/python3 - "$source_dir" "$staged" <<'PYTHON'
import shutil
import sys
import tempfile
import zipapp
from pathlib import Path

source, destination = map(Path, sys.argv[1:])
with tempfile.TemporaryDirectory(prefix="npu-lease-build-") as directory:
    staging = Path(directory)
    package = staging / "npu_lease"
    package.mkdir()
    for module in (source / "npu_lease").glob("*.py"):
        shutil.copyfile(module, package / module.name)
    shutil.copyfile(source / "npu-lease", staging / "__main__.py")
    zipapp.create_archive(staging, destination, interpreter="/usr/bin/python3")
PYTHON
chmod 0755 "$staged"
mv -T -- "$staged" "$bin_dir/npu-lease"
printf 'Installed %s\n' "$bin_dir/npu-lease"
