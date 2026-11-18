from __future__ import annotations

import hashlib
from pathlib import Path
from threading import Barrier
from urllib.parse import parse_qs, urlsplit

import pytest
import requests

from ukam_os_builder.api.settings import (
    MAX_DOWNLOAD_PARTS,
    Settings,
    SettingsError,
    load_settings,
)
from ukam_os_builder.os_builder import os_hub

DOWNLOAD_URL = "https://api.os.uk/downloads/v1/example?version=1"
FILE_BYTES = b"abcdefghijklmnopqrstuvwxyz0123456"
FILE_MD5 = hashlib.md5(FILE_BYTES, usedforsecurity=False).hexdigest()


def _response(status: int, body: bytes, **headers: str) -> requests.Response:
    response = requests.Response()
    response.status_code = status
    response.headers.update(headers)
    response._content = body
    response._content_consumed = True
    return response


def _range_response(requested: str) -> requests.Response:
    start, end = map(int, requested.removeprefix("bytes=").split("-"))
    return _response(
        206,
        FILE_BYTES[start : end + 1],
        **{"Content-Range": f"bytes {start}-{end}/{len(FILE_BYTES)}"},
    )


@pytest.mark.parametrize("download_parts", [None, 4], ids=["default", "opted-in"])
def test_download_step_honours_download_parts_config(
    temp_settings: Settings, monkeypatch: pytest.MonkeyPatch, download_parts: int | None
) -> None:
    options = f"  download_parts: {download_parts}\n" if download_parts is not None else ""
    temp_settings.config_path.write_text(
        'os_downloads:\n  package_id: "1"\n  version_id: "2"\n' + options
    )
    monkeypatch.setenv("OS_PROJECT_API_KEY", "test-key")
    settings = load_settings(temp_settings.config_path, load_env=False)
    expected_parts = download_parts or 1
    assert settings.os_downloads.download_parts == expected_parts
    filename = "add_isl_builtaddress.zip"
    metadata = {
        "downloads": [
            {"fileName": filename, "url": DOWNLOAD_URL, "size": len(FILE_BYTES), "md5": FILE_MD5}
        ]
    }
    monkeypatch.setattr(os_hub, "get_package_version", lambda _: metadata)
    monkeypatch.setattr(os_hub, "MIN_PARALLEL_DOWNLOAD_SIZE", 1)

    requests_seen = []
    barrier = Barrier(expected_parts, timeout=5)

    def get(self, url, *, stream, timeout, headers=None):
        assert stream
        assert parse_qs(urlsplit(url).query) == {"version": ["1"], "key": ["test-key"]}
        if expected_parts == 1:
            assert headers is None
            requests_seen.append(None)
            return _response(200, FILE_BYTES)
        assert headers["Accept-Encoding"] == "identity"
        requests_seen.append(headers["Range"])
        barrier.wait()
        return _range_response(headers["Range"])

    monkeypatch.setattr(requests.Session, "get", get)
    [output] = os_hub.run_download_step(settings)
    assert output.read_bytes() == FILE_BYTES
    assert list(output.parent.iterdir()) == [output]
    if expected_parts > 1:
        assert set(requests_seen) == {"bytes=0-7", "bytes=8-15", "bytes=16-23", "bytes=24-32"}
    else:
        assert requests_seen == [None]


@pytest.mark.parametrize("invalid_parts", [0, MAX_DOWNLOAD_PARTS + 1])
def test_download_parts_must_be_within_bounds(tmp_path: Path, invalid_parts: int) -> None:
    config = tmp_path / "config.yaml"
    config.write_text(
        f'os_downloads:\n  package_id: "1"\n  version_id: "2"\n  download_parts: {invalid_parts}\n'
    )

    with pytest.raises(SettingsError) as error:
        load_settings(config, load_env=False)

    assert "download_parts" in str(error.value.validation_error)


def test_ignored_range_falls_back_to_existing_whole_file_reader(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    monkeypatch.setattr(os_hub, "MIN_PARALLEL_DOWNLOAD_SIZE", 1)
    requests_seen = []

    def get(self, url, *, stream, timeout, headers=None):
        requests_seen.append(headers)
        response = _response(200, FILE_BYTES)
        if headers is not None:
            # A whole-file body returned to a range worker must not be consumed.
            def unexpected_read(chunk_size):
                pytest.fail("Read HTTP 200 response body in the range worker")

            response.iter_content = unexpected_read
        return response

    monkeypatch.setattr(requests.Session, "get", get)
    output = tmp_path / "data.zip"
    with caplog.at_level("INFO"):
        assert os_hub.download_file(
            DOWNLOAD_URL, output, "key", FILE_MD5, expected_size=len(FILE_BYTES), parts=4
        )
    assert len(requests_seen) > 1 and requests_seen[-1] is None
    assert output.read_bytes() == FILE_BYTES
    assert list(tmp_path.iterdir()) == [output]
    assert "Server ignored HTTP ranges" in caplog.text


def test_interrupted_range_retries_from_the_next_byte(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(os_hub, "MIN_PARALLEL_DOWNLOAD_SIZE", 1)
    requests_seen = []

    def get(self, url, *, stream, timeout, headers):
        requested = headers["Range"]
        requests_seen.append(requested)
        response = _range_response(requested)
        if requested == "bytes=0-15":

            def interrupted(chunk_size):
                yield FILE_BYTES[:4]
                raise requests.exceptions.ChunkedEncodingError("simulated interruption")

            response.iter_content = interrupted
        return response

    monkeypatch.setattr(requests.Session, "get", get)
    output = tmp_path / "data.zip"
    assert os_hub.download_file(
        DOWNLOAD_URL, output, "key", FILE_MD5, expected_size=len(FILE_BYTES), parts=2
    )
    assert set(requests_seen) == {"bytes=0-15", "bytes=4-15", "bytes=16-32"}
    assert output.read_bytes() == FILE_BYTES


@pytest.mark.parametrize("bad_header", [False, True], ids=["checksum", "content-range"])
def test_failed_parallel_download_preserves_existing_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, bad_header: bool
) -> None:
    monkeypatch.setattr(os_hub, "MIN_PARALLEL_DOWNLOAD_SIZE", 1)

    def get(self, url, *, stream, timeout, headers):
        response = _range_response(headers["Range"])
        if bad_header:
            response.headers["Content-Range"] = f"bytes 0-1/{len(FILE_BYTES)}"
        else:
            response._content = b"x" * len(response.content)
        return response

    monkeypatch.setattr(requests.Session, "get", get)
    output = tmp_path / "data.zip"
    output.write_bytes(b"previous output")
    with pytest.raises(ValueError, match="Content-Range" if bad_header else "MD5 mismatch"):
        os_hub.download_file(
            DOWNLOAD_URL,
            output,
            "key",
            FILE_MD5,
            force=True,
            expected_size=len(FILE_BYTES),
            parts=2,
        )
    assert output.read_bytes() == b"previous output"
    assert list(tmp_path.iterdir()) == [output]
