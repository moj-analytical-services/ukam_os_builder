from __future__ import annotations

import pytest

from ukam_os_builder.data_sources.ngd.ngd_exclusions import (
    ngd_file_matches_excluded_stem,
    parse_ngd_excluded_stems,
)


def test_ngd_excluded_stems_normalise_and_dedupe() -> None:
    assert parse_ngd_excluded_stems("HistoricAddress,*_ALTADD,historicaddress") == [
        "historicaddress",
        "*_altadd",
    ]


def test_ngd_core_exclusion_matches_exact_core_file_only() -> None:
    excluded = ["historicaddress"]

    assert ngd_file_matches_excluded_stem("add_gb_historicaddress.zip", excluded) is True
    assert (
        ngd_file_matches_excluded_stem("add_gb_historicaddress_altadd.zip", excluded)
        is False
    )


def test_ngd_altadd_exclusion_matches_all_alternate_address_files() -> None:
    excluded = ["*_altadd"]

    assert ngd_file_matches_excluded_stem("add_gb_builtaddress_altadd.zip", excluded) is True
    assert ngd_file_matches_excluded_stem("add_gb_historicaddress_altadd.zip", excluded) is True
    assert ngd_file_matches_excluded_stem("add_gb_historicaddress.zip", excluded) is False


def test_ngd_excluded_stems_reject_unknown_stem() -> None:
    with pytest.raises(ValueError, match="invalid NGD excluded stem"):
        parse_ngd_excluded_stems("not-a-feature")