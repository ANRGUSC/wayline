"""
Tar.gz pack/unpack helpers used by both Wayline and Argo wrappers.

The wire format between stages is a single tar.gz blob. Stages produce a
directory of files (frames, JSON, npy, etc.); wrappers tar+gzip it on the
producer side and untar on the consumer side. Inside the lib functions
there is no notion of tarballs — just input/output directories.
"""

from __future__ import annotations

import io
import tarfile
from pathlib import Path
from typing import Union


def pack_dir(src_dir: Union[str, Path]) -> bytes:
    """
    Tar+gzip every file under src_dir into an in-memory bytes blob. The
    archive is rooted at src_dir's basename so consumers can untar cleanly
    without colliding with sibling stages' artifacts.
    """
    src = Path(src_dir)
    if not src.is_dir():
        raise FileNotFoundError(f"pack_dir: not a directory: {src}")
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz", compresslevel=6) as tar:
        # arcname='.' keeps paths relative — every entry lands directly
        # under whatever dir the consumer extracts into.
        tar.add(str(src), arcname=".")
    return buf.getvalue()


def unpack_to_dir(blob: bytes, dest_dir: Union[str, Path]) -> None:
    """
    Extract a tar.gz blob into dest_dir. Creates dest_dir if missing. Uses
    `data` filter to refuse any entry with absolute paths, parent-relative
    paths, or special permissions — defense-in-depth against a malicious
    or malformed tar.
    """
    dest = Path(dest_dir)
    dest.mkdir(parents=True, exist_ok=True)
    buf = io.BytesIO(blob)
    with tarfile.open(fileobj=buf, mode="r:gz") as tar:
        # `data` filter (Python 3.12+) drops setuid, devices, absolute
        # paths, and `..` traversals. On older Python this is a no-op
        # warning; the lib targets 3.12 (Intel OpenVINO base ships it).
        try:
            tar.extractall(path=str(dest), filter="data")
        except TypeError:
            tar.extractall(path=str(dest))


def pack_dir_to_file(src_dir: Union[str, Path], out_path: Union[str, Path]) -> int:
    """Pack to a file path; return number of bytes written. Used by Argo
    wrappers whose contract is /out/output as a file."""
    blob = pack_dir(src_dir)
    out = Path(out_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_bytes(blob)
    return len(blob)


def unpack_file_to_dir(in_path: Union[str, Path], dest_dir: Union[str, Path]) -> None:
    """Unpack a tar.gz file path into a directory."""
    blob = Path(in_path).read_bytes()
    unpack_to_dir(blob, dest_dir)
