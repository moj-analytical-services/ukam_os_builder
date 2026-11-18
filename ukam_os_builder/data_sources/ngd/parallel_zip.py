"""Decode ZIP members with an optional rapidgzip executable."""

from __future__ import annotations

import logging
import shutil
import struct
import subprocess
import zipfile
from pathlib import Path

logger = logging.getLogger(__name__)

DECODER_THREADS = 16
_BLOCK_SIZE = 1024 * 1024
_ZIP_LOCAL_HEADER = struct.Struct("<26xHH")
_GZIP_HEADER = bytes.fromhex("1f8b08000000000000ff")


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
        # Gzip stores the uncompressed size modulo 2**32. Check the full size below.
        framed.write(struct.pack("<II", info.CRC, info.file_size & 0xFFFF_FFFF))


def copy_zip_member(
    archive_path: Path, member_name: str, csv_path: Path, rapidgzip: str | None
) -> None:
    """Write and verify a single member in a worker's temporary directory."""
    with zipfile.ZipFile(archive_path) as archive:
        info = archive.getinfo(member_name)
        # Opening with zipfile validates the local header and ZIP member bounds.
        with archive.open(info) as serial:
            if rapidgzip is not None and info.compress_type == zipfile.ZIP_DEFLATED:
                gzip_path = csv_path.with_suffix(".gz")
                try:
                    _write_gzip_member(archive_path, info, gzip_path)
                    process = subprocess.run(
                        [
                            rapidgzip,
                            "--decompress",
                            "--verify",
                            "--decoder-parallelism",
                            str(DECODER_THREADS),
                            "--output",
                            str(csv_path),
                            str(gzip_path),
                        ],
                        stdout=subprocess.DEVNULL,
                        stderr=subprocess.PIPE,
                        check=False,
                    )
                    if process.returncode:
                        raise zipfile.BadZipFile(
                            process.stderr.decode(errors="replace").strip()
                            or f"rapidgzip exited with status {process.returncode}"
                        )
                    if csv_path.stat().st_size != info.file_size:
                        raise zipfile.BadZipFile("Decompressed member size mismatch")
                    return
                except (OSError, struct.error, zipfile.BadZipFile) as exc:
                    logger.warning(
                        "rapidgzip could not decode %s in %s (%s). Retrying with the standard ZIP reader.",
                        member_name,
                        archive_path.name,
                        exc,
                    )
                finally:
                    gzip_path.unlink(missing_ok=True)

            # Truncate any partial output and let zipfile verify CRC on reading to EOF.
            with csv_path.open("wb") as output:
                shutil.copyfileobj(serial, output, length=_BLOCK_SIZE)
