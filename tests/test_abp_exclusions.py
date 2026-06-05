from __future__ import annotations

import pytest

from ukam_os_builder.data_sources.abp.abp_exclusions import (
    included_abp_logical_statuses,
    parse_abp_excluded_logical_statuses,
)


def test_abp_logical_statuses_include_historic_by_default() -> None:
    assert included_abp_logical_statuses([]) == [1, 3, 6, 8]


def test_abp_logical_status_exclusions_are_configurable() -> None:
    assert parse_abp_excluded_logical_statuses("8,3,8") == [8, 3]
    assert included_abp_logical_statuses([8]) == [1, 3, 6]


def test_abp_logical_status_exclusions_reject_unknown_status() -> None:
    with pytest.raises(ValueError, match="invalid ABP excluded logical status"):
        parse_abp_excluded_logical_statuses("2")
