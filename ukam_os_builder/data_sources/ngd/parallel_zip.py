"""Decode ZIP members with an optional rapidgzip executable."""

from __future__ import annotations

import errno
import logging
import os
import shutil
import struct
# Invokes the resolved rapidgzip binary directly with shell=False and fixed flags.
import subprocess  # nosec B404
import sys
import zipfile
from pathlib import Path

logger = logging.getLogger(__name__)

# Benchmarks on a 128-vCPU EC2 instance showed that increasing rapidgzip
# parallelism above 16 threads per member did not speed up CSV decompression
# when writing output to disk, so extra cores are better spent running more
# member jobs concurrently and feeding DuckDB.
DECODER_THREADS = 16
_BLOCK_SIZE = 1024 * 1024
_ZIP_LOCAL_HEADER = struct.Struct("<26xHH")
_GZIP_HEADER = bytes.fromhex("1f8b08000000000000ff")
_UNRECOVERABLE_ERRNOS = frozenset({errno.ENOSPC, errno.EDQUOT, errno.EACCES, errno.EROFS})


def find_rapidgzip() -> str | None:
    """Locate the rapidgzip executable in this Python environment or on PATH."""
    executable_dirs = os.pathsep.join(
        (str(Path(sys.executable).parent), os.environ.get("PATH", os.defpath))
    )
    return shutil.which("rapidgzip", path=executable_dirs)


def _is_unrecoverable_os_error(exc: BaseException) -> bool:
    return isinstance(exc, PermissionError) or (
        isinstance(exc, OSError) and exc.errno in _UNRECOVERABLE_ERRNOS
    )


def _error_from_rapidgzip_stderr(stderr_text: str) -> OSError | None:
    lowered = stderr_text.lower()
    if "no space left on device" in lowered:
        return OSError(errno.ENOSPC, stderr_text)
    if "disk quota exceeded" in lowered:
        return OSError(errno.EDQUOT, stderr_text)
    if "permission denied" in lowered:
        return PermissionError(errno.EACCES, stderr_text)
    if "read-only file system" in lowered:
        return OSError(errno.EROFS, stderr_text)
    return None


def _write_gzip_member(archive_path: Path, info: zipfile.ZipInfo, gzip_path: Path) -> None:
    """Give rapidgzip a seekable gzip file containing the member's original DEFLATE bytes."""
    with archive_path.open("rb") as source, gzip_path.open("xb") as framed:
        source.seek(info.header_offset)
        name_length, extra_length = _ZIP_LOCAL_HEADER.unpack(source.read(_ZIP_LOCAL_HEADER.size))
        source.seek(name_length + extra_length, 1)
        framed.write(_GZIP_HEADER)
        remaining = info.compress_size
        while remaining:
            block = source.read(min(remaining, _BLOCK_SIZE))
            if not block:
                raise zipfile.BadZipFile("Truncated compressed member")
            framed.write(block)
            remaining -= len(block)
        # Gzip stores the uncompressed size modulo 2**32. Check the full size after decode.
        framed.write(struct.pack("<II", info.CRC, info.file_size & 0xFFFF_FFFF))


def copy_zip_member(
    archive_path: Path,
    member_name: str,
    csv_path: Path,
    rapidgzip: str | None,
    *,
    scratch: Path,
    decoder_threads: int,
) -> None:
    """Write and verify a single member in a worker's temporary directory."""
    if decoder_threads < 1:
        raise ValueError("decoder_threads must be >= 1")

    with zipfile.ZipFile(archive_path) as archive:
        info = archive.getinfo(member_name)
        # Opening with zipfile validates the local header and ZIP member bounds.
        with archive.open(info) as serial:
            if rapidgzip is not None and info.compress_type == zipfile.ZIP_DEFLATED:
                gzip_path = scratch / "member.gz"
                try:
                    _write_gzip_member(archive_path, info, gzip_path)
                    # Resolved binary path, fixed flags, and resolved scratch paths with shell=False.
                    process = subprocess.run(  # nosec B603
                        [
                            rapidgzip,
                            "--decompress",
                            "--verify",
                            "--decoder-parallelism",
                            str(decoder_threads),
                            "--output",
                            str(csv_path.resolve()),
                            str(gzip_path.resolve()),
                        ],
                        stdout=subprocess.DEVNULL,
                        stderr=subprocess.PIPE,
                        check=False,
                    )
                    if process.returncode:
                        stderr_text = process.stderr.decode(errors="replace").strip()
                        unrecoverable = _error_from_rapidgzip_stderr(stderr_text)
                        if unrecoverable is not None:
                            raise unrecoverable
                        raise zipfile.BadZipFile(
                            stderr_text or f"rapidgzip exited with status {process.returncode}"
                        )
                    if csv_path.stat().st_size != info.file_size:
                        raise zipfile.BadZipFile("Decompressed member size mismatch")
                    return
                except (OSError, struct.error, zipfile.BadZipFile) as exc:
                    csv_path.unlink(missing_ok=True)
                    if _is_unrecoverable_os_error(exc):
                        raise
                    logger.warning(
                        "rapidgzip could not decode %s in %s (%s). "
                        "Retrying with the standard ZIP reader.",
                        member_name,
                        archive_path.name,
                        exc,
                    )
                finally:
                    gzip_path.unlink(missing_ok=True)

            # Truncate any partial output and let zipfile verify CRC on reading to EOF.
            with csv_path.open("wb") as output:
                shutil.copyfileobj(serial, output, length=_BLOCK_SIZE)
