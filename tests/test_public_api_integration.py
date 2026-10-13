from __future__ import annotations

import zipfile
from pathlib import Path
from textwrap import dedent

import duckdb
import pytest
import requests

from ukam_os_builder import inspect_flatfile_variants, run_from_config


def _write_config(path: Path, content: str) -> None:
    path.write_text(dedent(content).strip() + "\n", encoding="utf-8")


def test_configured_archive_path_feeds_next_stage(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    archives = tmp_path / "licensed" / "archives"
    archives.mkdir(parents=True)
    with zipfile.ZipFile(archives / "AddressBasePremium_local.zip", "w") as archive:
        archive.writestr("local.csv", "15,I,1,77,CONFIG ROAD,,LONDON,,ENG,,,,\n")
    config_path = tmp_path / "config.yaml"
    _write_config(
        config_path,
        """
        source:
          type: abp
        paths:
          work_dir: ./work
          overrides:
            downloads_dir: ./licensed/archives
            extracted_dir: ./staging/csv
            parquet_dir: ./staging/parquet
        os_downloads:
          package_id: local
          version_id: local
        """,
    )
    monkeypatch.setenv("OS_PROJECT_API_KEY", "test-key")
    monkeypatch.setenv("OS_PROJECT_API_SECRET", "test-secret")

    def reject_os_request(*_args: object, **_kwargs: object) -> None:
        pytest.fail("Processing configured local inputs must not contact OS Data Hub")

    monkeypatch.setattr(requests.sessions.Session, "request", reject_os_request)
    elsewhere = tmp_path / "elsewhere"
    elsewhere.mkdir()
    monkeypatch.chdir(elsewhere)

    run_from_config(config_path, step="extract", check_api=False)
    run_from_config(config_path, step="split", check_api=False)

    with duckdb.connect() as con:
        rows = con.execute(
            "SELECT usrn, street_description FROM read_parquet(?)",
            [str(tmp_path / "staging/parquet/raw/street_descriptor.parquet")],
        ).fetchall()
    assert rows == [(77, "CONFIG ROAD")]


def test_package_root_inspect_flatfile_variants(tmp_path: Path) -> None:
    output_dir = tmp_path / "out"
    output_dir.mkdir(parents=True)
    parquet_path = output_dir / "ngd_for_uk_address_matcher.chunk_001_of_001.parquet"

    con = duckdb.connect()
    con.execute(
        f"""
        COPY (
            SELECT * FROM (
                VALUES
                    (4001::BIGINT, 'A'::VARCHAR),
                    (4001::BIGINT, 'B'::VARCHAR),
                    (4002::BIGINT, 'C'::VARCHAR)
            ) AS t(unique_id, address_concat)
        ) TO '{parquet_path.as_posix()}' (FORMAT PARQUET)
        """
    )

    config_path = tmp_path / "config.yaml"
    _write_config(
        config_path,
        """
        source:
          type: ngd
        paths:
          output_dir: ./out
        """,
    )

    result = inspect_flatfile_variants(config_path=config_path, show=False)
    assert result["selected_uprn"] == 4001
    assert result["variant_count"] == 2
