"""The file operations a `copytruncate`, `gzip`, or `replace` step makes in a run's shared
directory, each the way the tool an operator runs makes it, with what it moved counted for the
ledger.

The writer's files belong to the `logit` user in its container, so the host user can't truncate
them. faults.py runs this file's source in a one-shot `python` container as root, with the shared
directory mounted, as `python -I -c <source> <op> <directory> <path> [<to>]`, and reads the one
JSON object it prints. It's stdlib only and imports nothing from the harness for that reason.

- `copytruncate` is logrotate's: it creates `to` empty, copies `path` into it until a read
  returns nothing, and truncates `path` in place, so a writer holding it `O_APPEND` goes on at
  offset 0. The bytes written after the copy's last read and before the truncate are in neither
  file. It reads them on its way to the truncate and counts their newlines as `lost_lines`; a
  write between that read and the truncate is lost uncounted, a window of two system calls.
- `gzip` is `gzip`'s: it creates `to`, writes `path` into it compressed, and removes `path`.
- `replace` is a restore or a migration: it writes `path`'s bytes to a dot-prefixed temporary
  name beside it, which an `app.log*` pattern doesn't match, and renames that over `path`, so the
  same content appears whole under a new inode.
"""

import gzip
import json
import os
import sys

CHUNK = 1 << 20


def _write_all(fd, data):
    view = memoryview(data)
    while view:
        view = view[os.write(fd, view):]


def _create(path, mode):
    return os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, mode)


def _own_like(fd, st):
    """Gives `fd` `st`'s owner, as logrotate does for its copy. Only root can, so elsewhere,
    the self-test, it keeps its own."""
    try:
        os.fchown(fd, st.st_uid, st.st_gid)
    except PermissionError:
        pass


def copytruncate(directory, path, to):
    source = os.path.join(directory, path)
    target = os.path.join(directory, to)
    fd = os.open(source, os.O_RDWR)
    try:
        st = os.fstat(fd)
        out = _create(target, st.st_mode & 0o777)
        copied = copied_lines = 0
        try:
            _own_like(out, st)
            while True:
                block = os.read(fd, CHUNK)
                if not block:
                    break
                _write_all(out, block)
                copied += len(block)
                copied_lines += block.count(b"\n")
        finally:
            os.close(out)
        lost = lost_lines = 0
        while True:
            block = os.read(fd, CHUNK)
            if not block:
                break
            lost += len(block)
            lost_lines += block.count(b"\n")
        os.ftruncate(fd, 0)
    finally:
        os.close(fd)
    return {"copied_bytes": copied, "copied_lines": copied_lines, "lost_bytes": lost,
            "lost_lines": lost_lines, "ino": st.st_ino, "to_ino": os.stat(target).st_ino}


def gzip_file(directory, path, to):
    source = os.path.join(directory, path)
    target = os.path.join(directory, to)
    size = lines = 0
    with open(source, "rb") as handle:
        st = os.fstat(handle.fileno())
        out = _create(target, st.st_mode & 0o777)
        _own_like(out, st)
        with os.fdopen(out, "wb") as raw, gzip.GzipFile(fileobj=raw, mode="wb",
                                                         filename="", mtime=0) as packed:
            while True:
                block = handle.read(CHUNK)
                if not block:
                    break
                packed.write(block)
                size += len(block)
                lines += block.count(b"\n")
    os.unlink(source)
    return {"bytes": size, "lines": lines, "gzip_bytes": os.stat(target).st_size,
            "ino": st.st_ino, "to_ino": os.stat(target).st_ino}


def replace(directory, path):
    source = os.path.join(directory, path)
    temporary = os.path.join(directory, f".{path}.restore")
    size = lines = 0
    with open(source, "rb") as handle:
        st = os.fstat(handle.fileno())
        out = _create(temporary, st.st_mode & 0o777)
        try:
            _own_like(out, st)
            while True:
                block = handle.read(CHUNK)
                if not block:
                    break
                _write_all(out, block)
                size += len(block)
                lines += block.count(b"\n")
        finally:
            os.close(out)
    os.rename(temporary, source)
    return {"bytes": size, "lines": lines, "ino": st.st_ino, "to_ino": os.stat(source).st_ino}


OPS = {"copytruncate": (copytruncate, 3), "gzip": (gzip_file, 3), "replace": (replace, 2)}


def main(argv):
    """`<op> <directory> <path> [<to>]`: prints the op's JSON object and returns 0, or prints
    the error to stderr and returns 1."""
    if not argv or argv[0] not in OPS or len(argv) - 1 != OPS[argv[0]][1]:
        print(f"usage: {' | '.join(OPS)} <directory> <path> [<to>], got {argv}",
              file=sys.stderr)
        return 1
    op, args = OPS[argv[0]][0], argv[1:]
    try:
        result = op(*args)
    except OSError as err:
        print(f"{argv[0]}: {err}", file=sys.stderr)
        return 1
    print(json.dumps(result))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
