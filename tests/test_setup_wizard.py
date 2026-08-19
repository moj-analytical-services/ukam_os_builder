from __future__ import annotations

from pathlib import Path

import pytest

from ukam_os_builder import setup_wizard


def _input_feeder(values: list[str]):
    iterator = iter(values)

    def _fake_input(_prompt: str, markup: bool = False) -> str:  # noqa: ARG001
        return next(iterator)

    return _fake_input


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

    captured: dict[str, object] = {}

    def fake_write_config_and_env(**kwargs):
        captured.update(kwargs)
        return Path(kwargs["config_out"]).resolve(), Path(kwargs["env_out"]).resolve(), True

    monkeypatch.setattr(setup_wizard, "write_config_and_env", fake_write_config_and_env)

    exit_code = setup_wizard.main(["--config-out", str(config_path), "--env-out", str(env_path)])

    assert exit_code == 0
    assert captured["write_env"] is True
    assert captured["overwrite_env"] is True
    assert captured["api_key"] == "new-key"
    assert captured["api_secret"] == "new-secret"


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
                "",  # source (default)
                "pkg-1",
                "ver-1",
                "",
                "n",  # advanced settings
                "n",  # setup .env now
            ]
        ),
    )

    captured: dict[str, object] = {}

    def fake_write_config_and_env(**kwargs):
        captured.update(kwargs)
        return Path(kwargs["config_out"]).resolve(), Path(kwargs["env_out"]).resolve(), False

    monkeypatch.setattr(setup_wizard, "write_config_and_env", fake_write_config_and_env)

    exit_code = setup_wizard.main(["--config-out", str(config_path), "--env-out", str(env_path)])

    assert exit_code == 0
    assert captured["write_env"] is False
    assert captured["api_key"] is None
    assert captured["api_secret"] is None


def test_setup_wizard_normalises_bare_memory_limit(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config_path = tmp_path / "config.yaml"
    env_path = tmp_path / ".env"

    monkeypatch.setattr(
        setup_wizard.console,
        "input",
        _input_feeder(["", "pkg-1", "ver-1", "", "y", "", "", "", "16", "n"]),
    )

    captured: dict[str, object] = {}

    def fake_write_config_and_env(**kwargs):
        captured.update(kwargs)
        return Path(kwargs["config_out"]).resolve(), Path(kwargs["env_out"]).resolve(), False

    monkeypatch.setattr(setup_wizard, "write_config_and_env", fake_write_config_and_env)

    exit_code = setup_wizard.main(["--config-out", str(config_path), "--env-out", str(env_path)])

    assert exit_code == 0
    config = captured["config"]
    assert isinstance(config, dict)
    assert config["processing"]["duckdb_memory_limit"] == "16GB"


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

    captured: dict[str, object] = {}

    def fake_write_config_and_env(**kwargs):
        captured.update(kwargs)
        return Path(kwargs["config_out"]).resolve(), Path(kwargs["env_out"]).resolve(), False

    monkeypatch.setattr(setup_wizard, "write_config_and_env", fake_write_config_and_env)

    exit_code = setup_wizard.main(["--config-out", str(config_path), "--env-out", str(env_path)])

    assert exit_code == 0
    assert captured["write_env"] is False
    assert captured["overwrite_env"] is False
