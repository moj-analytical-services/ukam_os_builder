from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from ukam_os_builder.os_builder.extract import (
    _filter_zips_for_source,
    _should_convert_csv_to_parquet,
)
from ukam_os_builder.os_builder.os_hub import _should_skip_ngd_download


def test_filter_zips_for_source_prefers_ngd_named_zips() -> None:
    zip_files = [
        Path("add_gb_builtaddress.zip"),
        Path("AddressBasePremium_FULL_2025-12-15_002.zip"),
    ]

    filtered = _filter_zips_for_source(zip_files, "ngd")

    assert filtered == [Path("add_gb_builtaddress.zip")]


def test_should_convert_csv_to_parquet_skips_non_ngd_for_ngd_source() -> None:
    ngd_csv = Path("add_gb_builtaddress.csv")
    abp_csv = Path("AddressBasePremium_FULL_2025-12-15_002.csv")

    assert _should_convert_csv_to_parquet(ngd_csv, "ngd") is True
    assert _should_convert_csv_to_parquet(abp_csv, "ngd") is False


def test_filter_zips_for_source_includes_historic_with_empty_exclusions() -> None:
    zip_files = [
        Path("add_gb_builtaddress.zip"),
        Path("add_gb_historicaddress.zip"),
        Path("add_gb_historicaddress_altadd.zip"),
        Path("add_gb_prebuildaddress.zip"),
    ]

    filtered = _filter_zips_for_source(zip_files, "ngd")

    assert Path("add_gb_builtaddress.zip") in filtered
    assert Path("add_gb_prebuildaddress.zip") in filtered
    assert Path("add_gb_historicaddress.zip") in filtered
    assert Path("add_gb_historicaddress_altadd.zip") in filtered


def test_filter_zips_for_source_uses_configured_ngd_exclusions() -> None:
    zip_files = [
        Path("add_gb_builtaddress.zip"),
        Path("add_gb_historicaddress.zip"),
        Path("add_gb_historicaddress_altadd.zip"),
        Path("add_gb_prebuildaddress_altadd.zip"),
    ]

    filtered = _filter_zips_for_source(
        zip_files,
        "ngd",
        ["historicaddress", "prebuildaddress"],
    )

    assert filtered == [Path("add_gb_builtaddress.zip")]


def test_core_ngd_exclusion_also_excludes_matching_altadd_files() -> None:
    zip_files = [
        Path("add_gb_historicaddress.zip"),
        Path("add_gb_historicaddress_altadd.zip"),
    ]

    filtered = _filter_zips_for_source(zip_files, "ngd", ["historicaddress"])

    assert filtered == []


def test_csv_to_parquet_includes_historic_with_empty_exclusions() -> None:
    assert _should_convert_csv_to_parquet(
        Path("add_gb_builtaddress.csv"),
        "ngd",
    ) is True
    assert _should_convert_csv_to_parquet(
        Path("add_gb_historicaddress.csv"),
        "ngd",
    ) is True
    assert _should_convert_csv_to_parquet(
        Path("add_gb_historicaddress_altadd.csv"),
        "ngd",
    ) is True


def test_should_convert_csv_to_parquet_uses_configured_ngd_exclusions() -> None:
    excluded = ["historicaddress"]

    assert _should_convert_csv_to_parquet(
        Path("add_gb_builtaddress.csv"),
        "ngd",
        excluded,
    ) is True
    assert (
        _should_convert_csv_to_parquet(
            Path("add_gb_historicaddress.csv"),
            "ngd",
            excluded,
        )
        is False
    )


def test_should_skip_ngd_download_uses_configured_exclusions() -> None:
    settings = SimpleNamespace(
        source=SimpleNamespace(type="ngd"),
        processing=SimpleNamespace(ngd_excluded_stems=["historicaddress"]),
    )

    assert _should_skip_ngd_download("add_gb_historicaddress.zip", settings) is True
    assert (
        _should_skip_ngd_download("add_gb_historicaddress_altadd.zip", settings)
        is True
    )
    assert _should_skip_ngd_download("add_gb_builtaddress.zip", settings) is False
