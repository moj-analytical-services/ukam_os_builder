from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path
from typing import Any

NGD_ADDRESS_PREFIXES = ("add_gb_", "add_isl_")
NGD_EXCLUDABLE_FEATURES = (
    "builtaddress",
    "prebuildaddress",
    "historicaddress",
    "nonaddressableobject",
    "royalmailaddress",
)

VALID_NGD_EXCLUDED_STEMS = frozenset(NGD_EXCLUDABLE_FEATURES)
DEFAULT_NGD_EXCLUDED_STEMS = ("historicaddress",)


def format_valid_ngd_excluded_stems() -> str:
    """Return valid NGD exclusion options for error messages and help text."""
    return ", ".join(NGD_EXCLUDABLE_FEATURES)


def normalise_ngd_excluded_stems(values: Iterable[Any] | None) -> list[str]:
    """Validate and normalise NGD exclusion option names."""
    if values is None:
        return []

    normalised: list[str] = []
    seen: set[str] = set()
    for raw_value in values:
        if not isinstance(raw_value, str):
            raise ValueError("ngd_excluded_stems entries must be strings")

        value = raw_value.strip().lower()
        if value not in VALID_NGD_EXCLUDED_STEMS:
            raise ValueError(
                "invalid NGD excluded stem "
                f"{raw_value!r}; valid options are: {format_valid_ngd_excluded_stems()}"
            )

        if value not in seen:
            normalised.append(value)
            seen.add(value)

    return normalised


def parse_ngd_excluded_stems(value: str | Iterable[Any] | None) -> list[str]:
    """Parse CLI/API exclusion input into validated option names."""
    if value is None:
        return []
    if isinstance(value, str):
        if not value.strip():
            return []
        return normalise_ngd_excluded_stems(part.strip() for part in value.split(","))
    return normalise_ngd_excluded_stems(value)


def get_configured_ngd_excluded_stems(settings: Any) -> list[str]:
    """Read configured NGD exclusions from a settings-like object."""
    processing = getattr(settings, "processing", None)
    return normalise_ngd_excluded_stems(
        getattr(processing, "ngd_excluded_stems", DEFAULT_NGD_EXCLUDED_STEMS)
    )


def is_ngd_address_file(name: str) -> bool:
    """Return True when a path name looks like an NGD address feature file."""
    return Path(name).name.lower().startswith(NGD_ADDRESS_PREFIXES)


def ngd_file_matches_excluded_stem(
    name: str,
    excluded_stems: Iterable[Any] | None,
) -> bool:
    """Return True when an NGD file name matches configured exclusions.

    Feature options match the core and alternate-address files for GB and Islands.
    """
    excluded = set(normalise_ngd_excluded_stems(excluded_stems))
    if not excluded:
        return False

    file_stem = Path(name).stem.lower()
    for prefix in NGD_ADDRESS_PREFIXES:
        if file_stem.startswith(prefix):
            return file_stem.removeprefix(prefix).removesuffix("_altadd") in excluded
    return False
