"""Decode ZIP members with an optional rapidgzip backend."""

from __future__ import annotations

import io
import logging
import os
import shutil
import struct
import zipfile
import zlib
from collections.abc import Iterator
from contextlib import closing
from pathlib import Path
from types import ModuleType
from typing import BinaryIO

logger = logging.getLogger(__name__)

DECODER_THREADS = 16
_BLOCK_SIZE = 1024 * 1024
_ZIP_LOCAL_HEADER = struct.Struct("<26xHH")
_GZIP_HEADER = bytes.fromhex("1f8b08000000000000ff")


class _ParallelDecodeError(Exception):
    """The optional decoder could not verify and decode a member."""


class _GzipMember(io.RawIOBase):
    """Present one ZIP member's raw DEFLATE bytes as gzip without copying them."""

    def __init__(self, source: BinaryIO, start: int, info: zipfile.ZipInfo) -> None:
        self.source = source
        self.start = start
        self.trailer_start = len(_GZIP_HEADER) + info.compress_size
        self.trailer = struct.pack("<II", info.CRC, info.file_size & 0xFFFF_FFFF)
        self.size = self.trailer_start + len(self.trailer)
        self.position = 0

    def readable(self) -> bool:
        return True

    def seekable(self) -> bool:
        return True

    def tell(self) -> int:
        return self.position

    def seek(self, offset: int, whence: int = os.SEEK_SET) -> int:
        if whence == os.SEEK_CUR:
            offset += self.position
        elif whence == os.SEEK_END:
            offset += self.size
        elif whence != os.SEEK_SET:
            raise ValueError(f"Unsupported seek mode: {whence}")
        if offset < 0:
            raise ValueError("Cannot seek before the start of a member")
        self.position = offset
        return offset

    def readinto(self, buffer: bytearray) -> int:
        position = self.position
        if position >= self.size or not buffer:
            return 0
        if position < len(_GZIP_HEADER):
            data = _GZIP_HEADER[position : position + len(buffer)]
            buffer[: len(data)] = data
            count = len(data)
        elif position < self.trailer_start:
            self.source.seek(self.start + position - len(_GZIP_HEADER))
            count = self.source.readinto(memoryview(buffer)[: self.trailer_start - position])
            if not count:
                raise zipfile.BadZipFile("Truncated compressed member")
        else:
            offset = position - self.trailer_start
            data = self.trailer[offset : offset + len(buffer)]
            buffer[: len(data)] = data
            count = len(data)
        self.position += count
        return count


def _parallel_blocks(path: Path, info: zipfile.ZipInfo, rapidgzip: ModuleType) -> Iterator[bytes]:
    try:
        with path.open("rb") as source:
            source.seek(info.header_offset)
            name_length, extra_length = _ZIP_LOCAL_HEADER.unpack(
                source.read(_ZIP_LOCAL_HEADER.size)
            )
            start = source.tell() + name_length + extra_length
            with (
                io.BufferedReader(_GzipMember(source, start, info)) as framed,
                rapidgzip.open(framed, parallelization=DECODER_THREADS) as decoded,
            ):
                while block := decoded.read(_BLOCK_SIZE):
                    yield block
    except (
        AttributeError,
        OSError,
        TypeError,
        ValueError,
        RuntimeError,
        struct.error,
        zipfile.BadZipFile,
    ) as exc:
        raise _ParallelDecodeError(str(exc)) from exc


def copy_zip_member(
    archive_path: Path, member_name: str, csv_path: Path, rapidgzip: ModuleType | None
) -> None:
    """Write and verify a single member in a worker's temporary directory."""
    with zipfile.ZipFile(archive_path) as archive, csv_path.open("wb") as output:
        info = archive.getinfo(member_name)
        # Opening with zipfile validates the local header and ZIP member bounds.
        with archive.open(info) as serial:
            if rapidgzip is not None and info.compress_type == zipfile.ZIP_DEFLATED:
                try:
                    size = crc = 0
                    with closing(_parallel_blocks(archive_path, info, rapidgzip)) as blocks:
                        for block in blocks:
                            size += len(block)
                            if size > info.file_size:
                                raise _ParallelDecodeError("Member exceeded its declared size")
                            crc = zlib.crc32(block, crc)
                            output.write(block)
                    if size != info.file_size or crc != info.CRC:
                        raise _ParallelDecodeError("Member size or CRC-32 mismatch")
                    return
                except _ParallelDecodeError as exc:
                    logger.warning(
                        "rapidgzip could not decode %s in %s (%s). Retrying with the standard ZIP reader.",
                        member_name,
                        archive_path.name,
                        exc,
                    )
                    output.seek(0)
                    output.truncate()

            # Reading to EOF lets zipfile verify the member CRC before parsing.
            shutil.copyfileobj(serial, output, length=_BLOCK_SIZE)
