"""Use an optional rapidgzip decoder behind the NGD ZIP filesystem."""

from __future__ import annotations

import io
import os
import struct
import zipfile
from contextlib import ExitStack
from pathlib import Path
from types import ModuleType
from typing import BinaryIO

from fsspec.implementations.zip import ZipFileSystem

ZIP_LOCAL_HEADER = struct.Struct("<4s22xHH")
GZIP_HEADER = bytes.fromhex("1f8b08000000000000ff")
MIN_PARALLEL_COMPRESSED_SIZE = 8 * 1024 * 1024


def supports_parallel_decompression(compress_type: int, compressed_size: int) -> bool:
    return compress_type == zipfile.ZIP_DEFLATED and compressed_size >= MIN_PARALLEL_COMPRESSED_SIZE


class _GzipMember(io.RawIOBase):
    """Present ZIP's raw DEFLATE bytes as gzip without copying the payload."""

    def __init__(self, source: BinaryIO, start: int, info: zipfile.ZipInfo) -> None:
        self.source = source
        self.start = start
        self.trailer_start = len(GZIP_HEADER) + info.compress_size
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
            offset += self.tell()
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
        if position < len(GZIP_HEADER):
            data = GZIP_HEADER[position : position + len(buffer)]
            buffer[: len(data)] = data
            count = len(data)
        elif position < self.trailer_start:
            self.source.seek(self.start + position - len(GZIP_HEADER))
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


class _ParallelMember(io.BufferedReader):
    def __init__(
        self,
        decoded: BinaryIO,
        resources: ExitStack,
        info: zipfile.ZipInfo,
        filesystem: ParallelZipFileSystem,
    ) -> None:
        super().__init__(decoded)
        self.resources = resources
        self.member_name = info.filename
        self.size = info.file_size
        self.filesystem = filesystem

    @property
    def name(self) -> str:
        return self.member_name

    def read(self, size: int = -1) -> bytes:
        try:
            return super().read(size)
        except (OSError, ValueError, RuntimeError):
            self.filesystem.decoder_failed = True
            raise

    def seek(self, offset: int, whence: int = os.SEEK_SET) -> int:
        try:
            return super().seek(offset, whence)
        except (OSError, ValueError, RuntimeError):
            self.filesystem.decoder_failed = True
            raise

    def close(self) -> None:
        try:
            super().close()
        finally:
            self.resources.close()


class ParallelZipFileSystem(ZipFileSystem):
    """Give each decoder its own file handle and preserve the ordinary ZIP reader."""

    def __init__(self, path: Path, source: BinaryIO, rapidgzip: ModuleType, threads: int) -> None:
        super().__init__(fo=source)
        self.path = path
        self.rapidgzip = rapidgzip
        self.threads = threads
        self.decoder_failed = False
        self.serial_members: set[str] = set()

    def _open(self, path: str, mode: str = "rb", **kwargs: object) -> BinaryIO:
        info = self.zip.getinfo(self._strip_protocol(path))
        if info.filename in self.serial_members or not supports_parallel_decompression(
            info.compress_type, info.compress_size
        ):
            return super()._open(path, mode, **kwargs)

        with ExitStack() as resources:
            try:
                # zipfile validates the local filename, flags and member bounds.
                self.zip.open(info).close()
                source = resources.enter_context(self.path.open("rb"))
                source.seek(info.header_offset)
                _, name_length, extra_length = ZIP_LOCAL_HEADER.unpack(
                    source.read(ZIP_LOCAL_HEADER.size)
                )
                start = source.tell() + name_length + extra_length
                framed = resources.enter_context(
                    io.BufferedReader(_GzipMember(source, start, info))
                )
                # Gzip framing lets rapidgzip verify the ZIP member's CRC and
                # avoids ambiguous detection of raw DEFLATE at the start.
                decoded = resources.enter_context(
                    self.rapidgzip.open(framed, parallelization=self.threads)
                )
            except (
                AttributeError,
                OSError,
                TypeError,
                ValueError,
                RuntimeError,
                zipfile.BadZipFile,
            ):
                self.decoder_failed = True
                raise
            return _ParallelMember(decoded, resources.pop_all(), info, self)
