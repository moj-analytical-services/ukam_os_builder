from __future__ import annotations

import pytest

from ukam_os_builder.data_sources.ngd.ngd_exclusions import (
    ngd_file_matches_excluded_stem,
    parse_ngd_excluded_stems,
)


def test_ngd_excluded_stems_normalise_and_dedupe() -> None:
    assert parse_ngd_excluded_stems("HistoricAddress,prebuildaddress,historicaddress") == [
        "historicaddress",
        "prebuildaddress",
    ]


def test_ngd_core_exclusion_matches_core_file_and_altadd_file() -> None:
    excluded = ["historicaddress"]

    assert ngd_file_matches_excluded_stem("add_gb_historicaddress.zip", excluded) is True
    assert (
        ngd_file_matches_excluded_stem("add_gb_historicaddress_altadd.zip", excluded)
        is True
    )


def test_ngd_feature_exclusion_only_matches_its_own_altadd_file() -> None:
    excluded = ["builtaddress"]

    assert ngd_file_matches_excluded_stem("add_gb_builtaddress_altadd.zip", excluded) is True
    assert ngd_file_matches_excluded_stem("add_gb_historicaddress_altadd.zip", excluded) is False
    assert ngd_file_matches_excluded_stem("add_gb_historicaddress.zip", excluded) is False


def test_ngd_excluded_stems_reject_unknown_stem() -> None:
    with pytest.raises(ValueError, match="invalid NGD excluded stem"):
        parse_ngd_excluded_stems("not-a-feature")

def test_ngd_excluded_stems_reject_altadd_wildcard() -> None:
    with pytest.raises(ValueError, match="invalid NGD excluded stem"):
        parse_ngd_excluded_stems("*_altadd")