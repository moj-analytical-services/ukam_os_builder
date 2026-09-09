from __future__ import annotations

import zipfile
from pathlib import Path

import duckdb
import pytest

from ukam_os_builder.api.settings import (
    OSDownloadSettings,
    PathSettings,
    ProcessingSettings,
    Settings,
    SourceSettings,
)
from ukam_os_builder.os_builder import extract


def _settings(tmp_path: Path, source: str = "ngd", exclusions: list[str] | None = None) -> Settings:
    downloads_dir = tmp_path / "downloads"
    extracted_dir = tmp_path / "extracted"
    downloads_dir.mkdir()

    return Settings(
        paths=PathSettings(
            work_dir=tmp_path,
            downloads_dir=downloads_dir,
            extracted_dir=extracted_dir,
            output_dir=tmp_path / "output",
        ),
        source=SourceSettings(type=source),
        os_downloads=OSDownloadSettings(package_id="test", version_id="test"),
        processing=ProcessingSettings(
            parquet_compression="zstd",
            parquet_compression_level=1,
            ngd_excluded_stems=exclusions or [],
        ),
        config_path=tmp_path / "config.yaml",
    )


def _write_zip(zip_path: Path, members: dict[str, str]) -> None:
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for member_name, content in members.items():
            archive.writestr(member_name, content)


def _count_rows(parquet_path: Path) -> int:
    con = duckdb.connect()
    try:
        return con.execute(
            "SELECT COUNT(*) FROM read_parquet(?)",
            [parquet_path.as_posix()],
        ).fetchone()[0]
    finally:
        con.close()


def test_ngd_converts_zip_members_without_extracting_csv(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    settings = _settings(tmp_path)
    _write_zip(
        settings.paths.downloads_dir / "add_gb_builtaddress.zip",
        {
            "nested/add_gb_builtaddress.csv": "uprn,fulladdress\n1,One Street\n2,Two Street\n",
            "nested/add_gb_builtaddress_altadd.csv": "uprn,fulladdress\n3,Three Street\n",
            "nested/add_gb_builtaddress_rltenty.csv": "uprn,related\n1,99\n",
        },
    )
    _write_zip(
        settings.paths.downloads_dir / "add_gb_streetaddress.zip",
        {"add_gb_streetaddress.csv": "streetid,name\n1,One Street\n"},
    )

    original_open = zipfile.ZipFile.open

    def open_consumed_member(archive, name, *args, **kwargs):
        member_name = name.filename if isinstance(name, zipfile.ZipInfo) else name
        assert not member_name.endswith(("_rltenty.csv", "streetaddress.csv"))
        return original_open(archive, name, *args, **kwargs)

    monkeypatch.setattr(zipfile.ZipFile, "open", open_consumed_member)
    outputs = extract.run_extract_step(settings, force=True)

    assert sorted(path.name for path in outputs) == [
        "add_gb_builtaddress.parquet",
        "add_gb_builtaddress_altadd.parquet",
    ]
    assert [_count_rows(path) for path in outputs] == [2, 1]
    assert not list(settings.paths.extracted_dir.rglob("*.csv"))


@pytest.mark.parametrize("stem", ["builtaddress_rltenty", "builtaddress_othcls", "streetaddress"])
def test_ngd_skips_archives_containing_only_unused_members(tmp_path: Path, stem: str) -> None:
    settings = _settings(tmp_path)
    _write_zip(
        settings.paths.downloads_dir / f"add_gb_{stem}.zip",
        {f"nested/add_gb_{stem}.csv": "unused,value\n1,ignored\n"},
    )

    assert extract.run_extract_step(settings) == []
    assert not list(settings.paths.extracted_dir.rglob("*.parquet"))
    assert not list(settings.paths.extracted_dir.rglob("*.csv"))


@pytest.mark.parametrize("fallback", [False, True])
@pytest.mark.parametrize("keep_all", [False, True])
def test_ngd_projection_preserves_values_and_source_column_order(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, fallback: bool, keep_all: bool
) -> None:
    settings = _settings(tmp_path)
    settings.processing.ngd_keep_all_columns = keep_all
    _write_zip(
        settings.paths.downloads_dir / "add_gb_builtaddress.zip",
        {
            "nested/add_gb_builtaddress.csv": 'FullAddress,Unused,UPRN,Postcode\n"One, Street",drop,1,AB1 2CD\n,drop,2,\n'
        },
    )
    if fallback:

        def fail_filesystem(*args: object, **kwargs: object) -> object:
            raise OSError("ZIP filesystem unavailable")

        monkeypatch.setattr(extract.fsspec, "filesystem", fail_filesystem)

    [output] = extract.run_extract_step(settings)
    with duckdb.connect() as con:
        actual = con.read_parquet(str(output))
        expected = con.sql("""
            SELECT * FROM (VALUES ('One, Street', 'drop', 1::BIGINT, 'AB1 2CD'),
                                  (NULL, 'drop', 2::BIGINT, NULL))
            t(FullAddress, Unused, UPRN, Postcode)
        """)
        if not keep_all:
            expected = expected.project("FullAddress, UPRN, Postcode")
        assert actual.columns == expected.columns
        assert actual.types == expected.types
        assert actual.order("UPRN").fetchall() == expected.order("UPRN").fetchall()


def test_ngd_projected_sources_preserve_complete_fixture_build(tmp_path: Path) -> None:
    from ukam_os_builder.data_sources.ngd.to_flatfile import run_flatfile_step

    outputs = []
    sources = []
    for keep_all in (True, False):
        work = tmp_path / str(keep_all)
        work.mkdir()
        settings = _settings(work)
        settings.processing.ngd_keep_all_columns = keep_all
        settings.processing.num_chunks = 2
        for csv in sorted((Path(__file__).parent / "data").glob("*.csv")):
            _write_zip(
                settings.paths.downloads_dir / f"{csv.stem}.zip",
                {f"nested/{csv.name}": csv.read_text()},
            )
        sources.append(extract.run_extract_step(settings))
        outputs.append(run_flatfile_step(settings))

    with duckdb.connect() as con:
        # Verify every retained field, including inferred types, in each source family.
        for full, narrow in zip(sources[0], sources[1], strict=True):
            left = con.read_parquet(str(full))
            right = con.read_parquet(str(narrow))
            assert len(right.columns) < len(left.columns)
            left = left.project(", ".join('"' + name + '"' for name in right.columns))
            assert left.columns == right.columns and left.types == right.types
            left.create_view("l", replace=True)
            right.create_view("r", replace=True)
            assert con.sql("FROM l EXCEPT ALL FROM r").fetchall() == []
            assert con.sql("FROM r EXCEPT ALL FROM l").fetchall() == []
        # The two chunks preserve all canonical values and multiplicities.
        for full, narrow in zip(outputs[0], outputs[1], strict=True):
            left = con.read_parquet(str(full))
            right = con.read_parquet(str(narrow))
            assert left.columns == right.columns and left.types == right.types
            left.create_view("l", replace=True)
            right.create_view("r", replace=True)
            assert con.sql("FROM l EXCEPT ALL FROM r").fetchall() == []
            assert con.sql("FROM r EXCEPT ALL FROM l").fetchall() == []


def test_ngd_raw_csv_extraction_keeps_side_members(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    members = {
        "add_gb_builtaddress.csv": "uprn,fulladdress\n1,One Street\n",
        "add_gb_builtaddress_rltenty.csv": "uprn,related\n1,99\n",
    }
    _write_zip(settings.paths.downloads_dir / "add_gb_builtaddress.zip", members)

    outputs = extract.run_extract_step(settings, force=True, convert_to_parquet=False)

    assert {path.name for path in outputs} == set(members)
    assert all(path.read_text() == members[path.name] for path in outputs)


def test_ngd_zip_member_exclusions_are_applied_before_conversion(tmp_path: Path) -> None:
    settings = _settings(tmp_path, exclusions=["historicaddress"])
    _write_zip(
        settings.paths.downloads_dir / "add_gb_builtaddress.zip",
        {
            "add_gb_builtaddress.csv": "uprn,fulladdress\n1,One Street\n",
            "add_gb_historicaddress.csv": "uprn,fulladdress\n2,Old Street\n",
        },
    )

    outputs = extract.run_extract_step(settings, force=True)

    assert [path.name for path in outputs] == ["add_gb_builtaddress.parquet"]


def test_abp_extract_still_materialises_csv(tmp_path: Path) -> None:
    settings = _settings(tmp_path, source="abp")
    _write_zip(
        settings.paths.downloads_dir / "AddressBasePremium_FULL_test.zip",
        {"AddressBasePremium_FULL_test.csv": "24,value\n"},
    )

    outputs = extract.run_extract_step(settings, force=True, convert_to_parquet=False)

    assert len(outputs) == 1
    assert outputs[0].suffix == ".csv"
    assert outputs[0].read_text() == "24,value\n"


def test_ngd_falls_back_to_csv_extraction_when_zip_filesystem_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    settings = _settings(tmp_path)
    _write_zip(
        settings.paths.downloads_dir / "add_gb_builtaddress.zip",
        {"add_gb_builtaddress.csv": "uprn,fulladdress\n1,One Street\n"},
    )

    def fail_filesystem(*args: object, **kwargs: object) -> object:
        raise OSError("ZIP filesystem unavailable")

    monkeypatch.setattr(extract.fsspec, "filesystem", fail_filesystem)

    with caplog.at_level("WARNING"):
        outputs = extract.run_extract_step(settings, force=True)

    assert [_count_rows(path) for path in outputs] == [1]
    assert list(settings.paths.extracted_dir.rglob("*.csv"))
    assert "falling back to CSV extraction" in caplog.text


def test_ngd_does_not_fallback_for_conversion_errors(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = _settings(tmp_path)
    _write_zip(
        settings.paths.downloads_dir / "add_gb_builtaddress.zip",
        {"add_gb_builtaddress.csv": "uprn,fulladdress\n1,One Street\n"},
    )

    def fail_conversion(*args: object, **kwargs: object) -> list[Path]:
        raise duckdb.ConversionException("bad CSV conversion")

    monkeypatch.setattr(extract, "_convert_zip_to_parquet_direct", fail_conversion)

    with pytest.raises(duckdb.ConversionException, match="bad CSV conversion"):
        extract.run_extract_step(settings, force=True)

    assert not list(settings.paths.extracted_dir.rglob("*.csv"))


def test_ngd_does_not_fallback_for_output_errors(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = _settings(tmp_path)
    _write_zip(
        settings.paths.downloads_dir / "add_gb_builtaddress.zip",
        {"add_gb_builtaddress.csv": "uprn,fulladdress\n1,One Street\n"},
    )

    def fail_output(*args: object, **kwargs: object) -> Path:
        raise duckdb.IOException("output directory unavailable")

    monkeypatch.setattr(extract, "_copy_csv_source_to_parquet", fail_output)

    with pytest.raises(duckdb.IOException, match="output directory unavailable"):
        extract.run_extract_step(settings, force=True)

    assert not list(settings.paths.extracted_dir.rglob("*.csv"))


def test_ngd_reuses_direct_parquet_outputs_without_extracting_csv(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    _write_zip(
        settings.paths.downloads_dir / "add_gb_builtaddress.zip",
        {"add_gb_builtaddress.csv": "uprn,fulladdress\n1,One Street\n"},
    )

    first_outputs = extract.run_extract_step(settings, force=True)
    second_outputs = extract.run_extract_step(settings, force=False)

    assert second_outputs == first_outputs
    assert not list(settings.paths.extracted_dir.rglob("*.csv"))


def test_ngd_rejects_duplicate_member_output_names(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    _write_zip(
        settings.paths.downloads_dir / "add_gb_builtaddress.zip",
        {
            "first/add_gb_builtaddress.csv": "uprn,fulladdress\n1,One Street\n",
            "second/add_gb_builtaddress.csv": "uprn,fulladdress\n2,Two Street\n",
        },
    )

    with pytest.raises(ValueError, match="same Parquet output"):
        extract.run_extract_step(settings, force=True)

    assert not list(settings.paths.extracted_dir.rglob("*.parquet"))
    assert not list(settings.paths.extracted_dir.rglob("*.csv"))


def test_ngd_rejects_duplicate_outputs_across_archives(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    _write_zip(
        settings.paths.downloads_dir / "add_gb_builtaddress.zip",
        {"add_gb_builtaddress.csv": "uprn,fulladdress\n1,One Street\n"},
    )
    _write_zip(
        settings.paths.downloads_dir / "add_gb_builtaddress_alt.zip",
        {"add_gb_builtaddress.csv": "uprn,fulladdress\n2,Two Street\n"},
    )

    with pytest.raises(ValueError, match="same Parquet output"):
        extract.run_extract_step(settings, force=True)

    assert not list(settings.paths.extracted_dir.rglob("*.parquet"))
    assert not list(settings.paths.extracted_dir.rglob("*.csv"))


def test_large_zip_csv_is_exact_with_multiple_writer_threads(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    settings = _settings(tmp_path)
    csv_path = tmp_path / "source.csv"
    expected_sql = (
        "SELECT i AS uprn, repeat('ADDRESS ', 20) || i::VARCHAR AS fulladdress "
        "FROM range(150000) AS t(i)"
    )
    with duckdb.connect() as con:
        con.execute(f"COPY ({expected_sql}) TO '{csv_path}' (HEADER)")
    with zipfile.ZipFile(
        settings.paths.downloads_dir / "add_gb_builtaddress.zip", "w", zipfile.ZIP_DEFLATED
    ) as archive:
        archive.write(csv_path, "add_gb_builtaddress.csv")

    original_connect = extract.create_duckdb_connection

    def parallel_connection(settings: Settings) -> duckdb.DuckDBPyConnection:
        con = original_connect(settings)
        con.execute("SET threads=8")
        return con

    monkeypatch.setattr(extract, "create_duckdb_connection", parallel_connection)
    [output] = extract.run_extract_step(settings)
    with duckdb.connect() as con:
        actual = con.read_parquet(str(output))
        assert actual.columns == ["uprn", "fulladdress"]
        assert [str(t) for t in actual.types] == ["BIGINT", "VARCHAR"]
        actual.create_view("actual")
        con.sql(expected_sql).create_view("expected")
        assert con.execute(
            "SELECT count(*) FROM ((FROM actual EXCEPT ALL FROM expected) "
            "UNION ALL (FROM expected EXCEPT ALL FROM actual))"
        ).fetchone() == (0,)
    assert not list(settings.paths.extracted_dir.rglob("*.csv"))
