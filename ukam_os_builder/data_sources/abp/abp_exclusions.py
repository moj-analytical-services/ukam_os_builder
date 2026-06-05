from __future__ import annotations

from collections.abc import Iterable
from typing import Any

ABP_SUPPORTED_LOGICAL_STATUSES = (1, 3, 6, 8)
VALID_ABP_EXCLUDED_LOGICAL_STATUSES = frozenset(ABP_SUPPORTED_LOGICAL_STATUSES)


def format_valid_abp_excluded_logical_statuses() -> str:
    """Return valid ABP logical status options for messages and help text."""
    return ", ".join(str(status) for status in ABP_SUPPORTED_LOGICAL_STATUSES)


def normalise_abp_excluded_logical_statuses(values: Iterable[Any] | None) -> list[int]:
    """Validate and normalise ABP logical statuses to exclude."""
    if values is None:
        return []

    normalised: list[int] = []
    seen: set[int] = set()
    for raw_value in values:
        if isinstance(raw_value, bool):
            raise ValueError("abp_excluded_logical_statuses entries must be integers")

        try:
            value = int(raw_value)
        except (TypeError, ValueError) as exc:
            raise ValueError(
                "abp_excluded_logical_statuses entries must be integers"
            ) from exc

        if value not in VALID_ABP_EXCLUDED_LOGICAL_STATUSES:
            raise ValueError(
                "invalid ABP excluded logical status "
                f"{raw_value!r}; valid options are: "
                f"{format_valid_abp_excluded_logical_statuses()}"
            )

        if value not in seen:
            normalised.append(value)
            seen.add(value)

    return normalised


def parse_abp_excluded_logical_statuses(value: str | Iterable[Any] | None) -> list[int]:
    """Parse CLI/API exclusion input into validated ABP logical status."""
    if value is None:
        return []
    if isinstance(value, str):
        if not value.strip():
            return []
        return normalise_abp_excluded_logical_statuses(part.strip() for part in value.split(","))
    return normalise_abp_excluded_logical_statuses(value)


def get_configured_abp_excluded_logical_statuses(settings: Any) -> list[int]:
    """Read configured ABP logical status exclusions from a settings-like object."""
    processing = getattr(settings, "processing", None)
    return normalise_abp_excluded_logical_statuses(
        getattr(processing, "abp_excluded_logical_statuses", [])
    )


def included_abp_logical_statuses(excluded_statuses: Iterable[Any] | None) -> list[int]:
    """Return supported ABP LPI logical status after configured exclusions."""
    excluded = set(normalise_abp_excluded_logical_statuses(excluded_statuses))
    return [status for status in ABP_SUPPORTED_LOGICAL_STATUSES if status not in excluded]
