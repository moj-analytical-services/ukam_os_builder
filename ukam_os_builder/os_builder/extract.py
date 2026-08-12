from __future__ import annotations

import logging
import os
import shutil
import tempfile
import zipfile
from dataclasses import dataclass
from pathlib import Path

import duckdb
import fsspec

from ukam_os_builder.api.settings import Settings, create_duckdb_connection
from ukam_os_builder.data_sources.ngd.ngd_exclusions import (
    get_configured_ngd_excluded_stems,
    is_ngd_address_file,
    ngd_file_matches_excluded_stem,
)

logger = logging.getLogger(__name__)


class ZipStreamError(RuntimeError):
    """Raised when a ZIP filesystem cannot be used for direct conversion."""


@dataclass(frozen=True)
class _ZipCsvMember:
    """CSV member metadata needed for direct ZIP-to-Parquet conversion."""

    name: str
    uri: str
    output_path: Path

def find_downloaded_zips(downloads_dir: Path) -> list[Path]:
    """Find all downloaded zip files in a directory."""
    if not downloads_dir.exists():
        return []

    zip_files = list(downloads_dir.glob("*.zip"))
    zip_files.sort()
    return zip_files


def _filter_zips_for_source(
    zip_files: list[Path],
    source: str,
    ngd_excluded_stems: list[str] | None = None,
) -> list[Path]:
    source_lower = source.lower()
    if source_lower == "ngd":
        ngd_zips = [zip_path for zip_path in zip_files if is_ngd_address_file(zip_path.name)]
        if not ngd_zips:
            return zip_files
        return [
            zip_path
            for zip_path in ngd_zips
            if not ngd_file_matches_excluded_stem(zip_path.name, ngd_excluded_stems)
        ]
    if source_lower == "abp":
        abp_zips = [
            zip_path for zip_path in zip_files if "addressbasepremium" in zip_path.name.lower()
        ]
        return abp_zips or zip_files
    return zip_files


def _should_convert_csv_to_parquet(
    csv_path: Path,
    source: str,
    ngd_excluded_stems: list[str] | None = None,
) -> bool:
    if source.lower() == "ngd":
        return is_ngd_address_file(csv_path.name) and not ngd_file_matches_excluded_stem(
            csv_path.name,
            ngd_excluded_stems,
        )
    return True


def extract_zip_to_csv(
    zip_path: Path,
    extracted_dir: Path,
    force: bool = False,
) -> list[Path]:
    """Extract CSV files from a zip archive.

    Args:
        zip_path: Path to the zip file.
        extracted_dir: Directory to extract to.
        force: Force re-extraction even if files exist.

    Returns:
        List of paths to extracted CSV files.

    Raises:
        FileNotFoundError: If zip file doesn't exist.
        zipfile.BadZipFile: If zip file is corrupted.
    """
    if not zip_path.exists():
        raise FileNotFoundError(f"Zip file not found: {zip_path}")

    # Create extraction subdirectory named after the zip file
    extract_subdir = extracted_dir / zip_path.stem

    # Check if already extracted
    existing_csvs = list(extract_subdir.rglob("*.csv")) if extract_subdir.exists() else []
    if existing_csvs and not force:
        logger.info("Already extracted %d CSV files from: %s", len(existing_csvs), zip_path.name)
        return existing_csvs

    # Clear existing directory on force
    if extract_subdir.exists() and force:
        logger.info("Removing existing extraction: %s", extract_subdir)
        shutil.rmtree(extract_subdir)

    # Extract only CSV files
    extract_subdir.mkdir(parents=True, exist_ok=True)
    logger.info("Extracting CSV files from %s to %s...", zip_path.name, extract_subdir)

    csv_paths: list[Path] = []
    with zipfile.ZipFile(zip_path, "r") as zf:
        for info in zf.infolist():
            name = info.filename

            # Skip directory entries
            if name.endswith("/"):
                continue

            # Filter to only CSV files
            if not name.lower().endswith(".csv"):
                continue

            # Extract file
            out_path = extract_subdir / name
            out_path.parent.mkdir(parents=True, exist_ok=True)

            with zf.open(info) as src, open(out_path, "wb") as dst:
                shutil.copyfileobj(src, dst)

            csv_paths.append(out_path)

    logger.info("Extraction complete: %d CSV files", len(csv_paths))
    return csv_paths


def _sql_string(value: str) -> str:
    return value.replace("'", "''")


def _processing_parquet_options(settings: Settings | None) -> tuple[str, int]:
    if settings is None:
        return "zstd", 9

    return (
        settings.processing.parquet_compression,
        settings.processing.parquet_compression_level,
    )


def _temporary_output_path(output_path: Path) -> Path:
    file_descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{output_path.name}.",
        suffix=".tmp",
        dir=output_path.parent,
    )
    os.close(file_descriptor)
    temporary_path = Path(temporary_name)
    temporary_path.unlink()
    return temporary_path


def _copy_csv_source_to_parquet(
    con: duckdb.DuckDBPyConnection,
    csv_source: str,
    output_path: Path,
    force: bool,
    settings: Settings | None = None,
) -> Path:
    if output_path.exists() and not force:
        logger.debug("Parquet file already exists: %s", output_path.name)
        return output_path

    output_path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = _temporary_output_path(output_path)
    compression, compression_level = _processing_parquet_options(settings)

    logger.debug("Converting %s -> %s", csv_source, output_path.name)

    try:
        con.execute(
            f"""
            COPY (
                SELECT * FROM read_csv_auto(?, sample_size=1000000)
            ) TO '{_sql_string(temporary_path.as_posix())}' (
                FORMAT 'PARQUET',
                COMPRESSION '{_sql_string(compression)}',
                COMPRESSION_LEVEL {compression_level}
            );
            """,
            [csv_source],
        )
        os.replace(temporary_path, output_path)
    except Exception:
        temporary_path.unlink(missing_ok=True)
        raise

    return output_path


def convert_csv_to_parquet(
    csv_path: Path,
    output_path: Path,
    force: bool = False,
    settings: Settings | None = None,
) -> Path:
    """Convert a CSV file to parquet format.

    Args:
        csv_path: Path to the CSV file.
        output_path: Path for the output parquet file.
        force: Force reconversion even if file exists.

    Returns:
        Path to the output parquet file.
    """
    con = create_duckdb_connection(settings) if settings is not None else duckdb.connect()
    try:
        return _copy_csv_source_to_parquet(
            con=con,
            csv_source=csv_path.as_posix(),
            output_path=output_path,
            force=force,
            settings=settings,
        )
    finally:
        con.close()


def _discover_zip_csv_members(
    zip_path: Path,
    extracted_dir: Path,
    source: str,
    ngd_excluded_stems: list[str] | None = None,
) -> list[_ZipCsvMember]:
    if not zip_path.exists():
        raise FileNotFoundError(f"Zip file not found: {zip_path}")

    try:
        with zipfile.ZipFile(zip_path) as zip_file:
            infos = zip_file.infolist()
    except zipfile.BadZipFile as exc:
        raise ValueError(f"Invalid ZIP archive {zip_path}: {exc}") from exc

    parquet_dir = extracted_dir / "parquet"
    members: list[_ZipCsvMember] = []
    output_owners: dict[Path, str] = {}

    for info in infos:
        member_name = info.filename
        member_path = Path(member_name)
        if member_name.endswith("/") or not member_name.lower().endswith(".csv"):
            continue
        if not _should_convert_csv_to_parquet(
            member_path,
            source,
            ngd_excluded_stems,
        ):
            continue

        output_path = parquet_dir / f"{member_path.stem}.parquet"
        previous_owner = output_owners.get(output_path)
        if previous_owner is not None:
            raise ValueError(
                "Duplicate CSV members map to the same Parquet output "
                f"{output_path.name}: {previous_owner!r} and {member_name!r}"
            )
        output_owners[output_path] = member_name
        members.append(
            _ZipCsvMember(
                name=member_name,
                uri=f"zip://{member_name}",
                output_path=output_path,
            )
        )

    return members


def _convert_extracted_csvs_to_parquet(
    csv_paths: list[Path],
    parquet_dir: Path,
    settings: Settings,
    source: str,
    ngd_excluded_stems: list[str],
    force: bool,
) -> list[Path]:
    parquet_files: list[Path] = []
    output_owners: dict[Path, Path] = {}
    for csv_path in csv_paths:
        if not _should_convert_csv_to_parquet(csv_path, source, ngd_excluded_stems):
            logger.debug("Skipping CSV-to-parquet for source '%s': %s", source, csv_path)
            continue

        parquet_path = parquet_dir / f"{csv_path.stem}.parquet"
        previous_owner = output_owners.get(parquet_path)
        if previous_owner is not None:
            raise ValueError(
                "CSV files map to the same Parquet output "
                f"{parquet_path.name}: {previous_owner} and {csv_path}"
            )
        output_owners[parquet_path] = csv_path
        convert_csv_to_parquet(csv_path, parquet_path, force=force, settings=settings)
        parquet_files.append(parquet_path)

    return parquet_files


def _convert_zip_to_parquet_direct(
    zip_path: Path,
    members: list[_ZipCsvMember],
    settings: Settings,
    force: bool,
) -> list[Path]:
    try:
        filesystem = fsspec.filesystem("zip", fo=str(zip_path))
    except Exception as exc:
        raise ZipStreamError(f"Could not open ZIP filesystem for {zip_path}: {exc}") from exc

    con = create_duckdb_connection(settings)
    try:
        try:
            con.register_filesystem(filesystem)
        except Exception as exc:
            raise ZipStreamError(
                f"Could not register ZIP filesystem for {zip_path}: {exc}"
            ) from exc

        parquet_files: list[Path] = []
        for member in members:
            try:
                with filesystem.open(member.name, "rb") as member_stream:
                    member_stream.read(1)
            except Exception as exc:
                raise ZipStreamError(
                    f"Could not read CSV member {member.name!r} from {zip_path}: {exc}"
                ) from exc
            parquet_files.append(
                _copy_csv_source_to_parquet(
                    con=con,
                    csv_source=member.uri,
                    output_path=member.output_path,
                    force=force,
                    settings=settings,
                )
            )
        return parquet_files
    finally:
        try:
            con.close()
        except Exception:
            logger.debug("Could not close DuckDB connection", exc_info=True)
        close_filesystem = getattr(filesystem, "close", None)
        if close_filesystem is not None:
            try:
                close_filesystem()
            except Exception:
                logger.debug("Could not close ZIP filesystem", exc_info=True)


def _convert_zip_to_parquet_with_fallback(
    zip_path: Path,
    extracted_dir: Path,
    settings: Settings,
    source: str,
    ngd_excluded_stems: list[str],
    force: bool,
    members: list[_ZipCsvMember],
) -> list[Path]:
    try:
        return _convert_zip_to_parquet_direct(zip_path, members, settings, force)
    except ZipStreamError as exc:
        logger.warning(
            "Direct ZIP-to-Parquet conversion failed for %s: %s; "
            "falling back to CSV extraction",
            zip_path.name,
            exc,
        )

    csv_paths = extract_zip_to_csv(zip_path, extracted_dir, force=force)
    return _convert_extracted_csvs_to_parquet(
        csv_paths=csv_paths,
        parquet_dir=extracted_dir / "parquet",
        settings=settings,
        source=source,
        ngd_excluded_stems=ngd_excluded_stems,
        force=force,
    )


def discover_raw_csv_files(extracted_dir: Path) -> list[Path]:
    """Discover raw ABP CSV files in the extracted directory.

    The ABP data comes as multiple CSV files (chunks) that need to be
    processed together.

    Args:
        extracted_dir: Directory containing extracted files.

    Returns:
        List of paths to CSV files to process.
    """
    if not extracted_dir.exists():
        logger.warning("Extracted directory does not exist: %s", extracted_dir)
        return []

    # Find all CSV files recursively
    csv_files = list(extracted_dir.rglob("*.csv"))

    # Sort for deterministic ordering
    csv_files.sort()

    logger.info("Discovered %d CSV file(s) in %s", len(csv_files), extracted_dir)
    for f in csv_files[:5]:  # Log first few
        logger.debug("  %s", f.name)
    if len(csv_files) > 5:
        logger.debug("  ... and %d more", len(csv_files) - 5)

    return csv_files


def run_extract_step(
    settings: Settings, force: bool = False, convert_to_parquet: bool = True
) -> list[Path]:
    """Run the extract step of the pipeline.

    Extracts all downloaded zip files and converts CSVs to parquet.

    Args:
        settings: Application settings.
        force: Force re-extraction even if files exist.

    Returns:
        List of parquet file paths.
    """
    downloads_dir = settings.paths.downloads_dir
    extracted_dir = settings.paths.extracted_dir

    # Ensure directories exist
    extracted_dir.mkdir(parents=True, exist_ok=True)

    # Find downloaded zips
    zip_files = find_downloaded_zips(downloads_dir)
    if not zip_files:
        logger.warning("No zip files found in %s. Run --step download first.", downloads_dir)
        return []

    source_type = settings.source.type
    ngd_excluded_stems = get_configured_ngd_excluded_stems(settings)
    filtered_zip_files = _filter_zips_for_source(zip_files, source_type, ngd_excluded_stems)
    if len(filtered_zip_files) != len(zip_files):
        logger.info(
            "Filtered %d zip file(s) for source '%s' (from %d total)",
            len(filtered_zip_files),
            source_type,
            len(zip_files),
        )
    zip_files = filtered_zip_files

    logger.info("Found %d zip file(s) to extract", len(zip_files))

    if convert_to_parquet and source_type.lower() == "ngd":
        archive_members: list[tuple[Path, list[_ZipCsvMember]]] = []
        output_owners: dict[Path, tuple[Path, str]] = {}
        for zip_path in zip_files:
            members = _discover_zip_csv_members(
                zip_path,
                extracted_dir,
                source_type,
                ngd_excluded_stems,
            )
            if not members:
                raise ValueError(f"No eligible CSV members found in {zip_path}")

            for member in members:
                previous_owner = output_owners.get(member.output_path)
                if previous_owner is not None:
                    previous_zip, previous_member = previous_owner
                    raise ValueError(
                        "CSV members from different ZIP archives map to the same "
                        f"Parquet output {member.output_path.name}: "
                        f"{previous_zip.name}!{previous_member} and "
                        f"{zip_path.name}!{member.name}"
                    )
                output_owners[member.output_path] = (zip_path, member.name)
            archive_members.append((zip_path, members))

        parquet_files: list[Path] = []
        for zip_path, members in archive_members:
            parquet_files.extend(
                _convert_zip_to_parquet_with_fallback(
                    zip_path=zip_path,
                    extracted_dir=extracted_dir,
                    settings=settings,
                    source=source_type,
                    ngd_excluded_stems=ngd_excluded_stems,
                    force=force,
                    members=members,
                )
            )

        logger.info("Extraction complete: %d parquet files", len(parquet_files))
        return parquet_files

    extracted_csvs: list[Path] = []
    for zip_path in zip_files:
        extracted_csvs.extend(extract_zip_to_csv(zip_path, extracted_dir, force=force))

    if not convert_to_parquet:
        logger.info("Extraction complete: %d CSV files", len(extracted_csvs))
        return extracted_csvs

    parquet_files = _convert_extracted_csvs_to_parquet(
        csv_paths=extracted_csvs,
        parquet_dir=extracted_dir / "parquet",
        settings=settings,
        source=source_type,
        ngd_excluded_stems=ngd_excluded_stems,
        force=force,
    )
    logger.info("Extraction complete: %d parquet files", len(parquet_files))
    return parquet_files


def get_parquet_dir(settings: Settings) -> Path:
    """Get the directory containing extracted parquet files.

    Args:
        settings: Application settings.

    Returns:
        Path to the parquet directory.
    """
    return settings.paths.extracted_dir / "parquet"
