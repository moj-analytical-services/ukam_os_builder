from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from ukam_os_builder import setup_wizard
from ukam_os_builder.api.settings import load_settings


def _input_feeder(values: list[str]):
    iterator = iter(values)

    def _fake_input(_prompt: str, markup: bool = False) -> str:  # noqa: ARG001
        return next(iterator)

    return _fake_input


@pytest.mark.parametrize("non_interactive", [False, True])
def test_setup_wizard_preserves_cloud_base_cache_and_overrides(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, non_interactive: bool
) -> None:
    config_path = tmp_path / "config.yaml"
    paths = {
        "work_dir": "s3://test-bucket/base",
        "cache_dir": "./scratch",
        "overrides": {"output_dir": "s3://test-bucket/final"},
    }
    config_path.write_text(yaml.safe_dump({"paths": paths}), encoding="utf-8")
    monkeypatch.setattr(
        setup_wizard.console, "input", _input_feeder(["", "", "pkg-1", "ver-1", "", "n", "n"])
    )
    arguments = ["--config-out", str(config_path), "--env-out", str(tmp_path / ".env")]
    if non_interactive:
        arguments.extend(
            [
                "--non-interactive",
                "--source",
                "ngd",
                "--package-id",
                "pkg-1",
                "--version-id",
                "ver-1",
            ]
        )

    assert setup_wizard.main(arguments) == 0
    settings = load_settings(config_path, load_env=False)
    assert settings.paths.work_dir == tmp_path / "scratch"
    assert settings.paths.downloads_dir == tmp_path / "scratch/downloads"
    assert settings.paths.output_dir == "s3://test-bucket/final"


def test_setup_wizard_prompts_for_env_credentials_and_overwrites_existing(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config_path = tmp_path / "config.yaml"
    env_path = tmp_path / ".env"
    env_path.write_text("OS_PROJECT_API_KEY=old\nOS_PROJECT_API_SECRET=old\n", encoding="utf-8")

    monkeypatch.setattr(
        setup_wizard.console,
        "input",
        _input_feeder(
            [
                "",  # download from OS Data Hub (default)
                "",  # source (default)
                "pkg-1",
                "ver-1",
                "",
                "n",  # advanced settings
                "y",  # setup .env now
                "y",  # overwrite existing .env
                "new-key",
                "new-secret",
            ]
        ),
    )

    exit_code = setup_wizard.main(["--config-out", str(config_path), "--env-out", str(env_path)])

    assert exit_code == 0
    assert "OS_PROJECT_API_KEY=new-key" in env_path.read_text().splitlines()
    assert "OS_PROJECT_API_SECRET=new-secret" in env_path.read_text().splitlines()


def test_setup_wizard_skips_env_update_when_user_declines(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config_path = tmp_path / "config.yaml"
    env_path = tmp_path / ".env"

    monkeypatch.setattr(
        setup_wizard.console,
        "input",
        _input_feeder(
            [
                "",  # download from OS Data Hub (default)
                "",  # source (default)
                "pkg-1",
                "ver-1",
                "",
                "n",  # advanced settings
                "n",  # setup .env now
            ]
        ),
    )

    exit_code = setup_wizard.main(["--config-out", str(config_path), "--env-out", str(env_path)])

    assert exit_code == 0
    assert not env_path.exists()


def test_setup_wizard_normalises_bare_memory_limit(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config_path = tmp_path / "config.yaml"
    env_path = tmp_path / ".env"

    monkeypatch.setattr(
        setup_wizard.console,
        "input",
        _input_feeder(["", "", "pkg-1", "ver-1", "", "y", "", "", "", "16", "n"]),
    )

    exit_code = setup_wizard.main(["--config-out", str(config_path), "--env-out", str(env_path)])

    assert exit_code == 0
    settings = load_settings(config_path, load_env=False)
    assert settings.processing.duckdb_memory_limit == "16GB"


def test_setup_wizard_accepts_only_gigabyte_memory_limits(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(setup_wizard.console, "input", _input_feeder(["16MB", "16GB"]))

    assert setup_wizard._prompt_memory_limit("memory limit") == "16GB"


def test_setup_wizard_decline_overwrite_keeps_existing_env(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config_path = tmp_path / "config.yaml"
    env_path = tmp_path / ".env"
    env_path.write_text("OS_PROJECT_API_KEY=old\nOS_PROJECT_API_SECRET=old\n", encoding="utf-8")

    monkeypatch.setattr(
        setup_wizard.console,
        "input",
        _input_feeder(
            [
                "",  # download from OS Data Hub (default)
                "",  # source (default)
                "pkg-1",
                "ver-1",
                "",
                "n",  # advanced settings
                "y",  # setup .env now
                "n",  # do not overwrite existing .env
            ]
        ),
    )

    exit_code = setup_wizard.main(["--config-out", str(config_path), "--env-out", str(env_path)])

    assert exit_code == 0
    assert env_path.read_text() == "OS_PROJECT_API_KEY=old\nOS_PROJECT_API_SECRET=old\n"


@pytest.mark.parametrize("source, existing_config", [("ngd", False), ("abp", True)])
def test_setup_wizard_local_archives_without_credential_updates(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, source: str, existing_config: bool
) -> None:
    config_path = tmp_path / "config.yaml"
    env_path = tmp_path / ".env"
    if existing_config:
        config_path.write_text(
            yaml.safe_dump(
                {
                    "paths": {"overrides": {"output_dir": "./results", "downloads_dir": None}},
                    "os_downloads": {"package_id": "pkg-1", "version_id": "ver-1"},
                }
            ),
            encoding="utf-8",
        )
        env_path.write_text("OS_PROJECT_API_KEY=keep\nOS_PROJECT_API_SECRET=keep\n")
    inputs = ["n", source, "./work"]
    if existing_config:
        inputs.append("")  # A null override must not become a directory named "None".
    inputs.extend(["./archives", "n"])
    monkeypatch.setattr(setup_wizard.console, "input", _input_feeder(inputs))

    assert (
        setup_wizard.main(
            ["--config-out", str(config_path), "--env-out", str(env_path), "--overwrite-env"]
        )
        == 0
    )
    settings = load_settings(config_path, load_env=False)
    assert settings.source.type == source
    assert settings.paths.downloads_dir == tmp_path / "archives"
    if existing_config:
        assert settings.paths.output_dir == tmp_path / "results"
        assert (settings.os_downloads.package_id, settings.os_downloads.version_id) == (
            "pkg-1",
            "ver-1",
        )
        assert env_path.read_text() == "OS_PROJECT_API_KEY=keep\nOS_PROJECT_API_SECRET=keep\n"
    else:
        assert not env_path.exists()
