from __future__ import annotations

from ukam_os_builder import cli


def test_build_cli_passes_api_credentials_to_run_from_config(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_run_from_config(**kwargs):
        captured.update(kwargs)
        return None

    monkeypatch.setattr(cli, "run_from_config", fake_run_from_config)
    monkeypatch.setattr(cli, "_configure_logging", lambda _verbose: None)

    exit_code = cli.main(
        [
            "--config",
            "config.yaml",
            "--step",
            "download",
            "--list-only",
            "--api-key",
            "runtime-key",
            "--api-secret",
            "runtime-secret",
            "--ngd-excluded-stems",
            "historicaddress,prebuildaddress",
            "--abp-excluded-logical-statuses",
            "8,3",
        ]
    )

    assert exit_code == 0
    assert captured["api_key"] == "runtime-key"
    assert captured["api_secret"] == "runtime-secret"
    assert captured["ngd_excluded_stems"] == "historicaddress,prebuildaddress"
    assert captured["abp_excluded_logical_statuses"] == "8,3"
