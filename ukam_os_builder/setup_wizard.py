from __future__ import annotations

import argparse
import re
from pathlib import Path

from rich.console import Console
from rich.panel import Panel

from ukam_os_builder.api.api import load_existing_defaults, write_config_and_env

console = Console()


def _prompt_non_empty(label: str, default: str = "") -> str:
    while True:
        suffix = f" [{default}]" if default else ""
        value = console.input(f"{label}{suffix}: ", markup=False).strip() or default
        if value:
            return value
        console.print("[red]Value is required.[/red]")


def _prompt_optional(label: str, default: str = "") -> str | None:
    suffix = f" [{default}]" if default else ""
    value = console.input(f"{label}{suffix}: ", markup=False).strip()
    if value:
        return value
    return default or None


def _prompt_memory_limit(label: str, default: str = "") -> str | None:
    while True:
        value = _prompt_optional(label, default)
        if value is None:
            return None
        if re.fullmatch(r"\d+(?:\.\d+)?", value):
            return f"{value}GB"
        match = re.fullmatch(r"(?P<amount>\d+(?:\.\d+)?)\s*GB", value, re.IGNORECASE)
        if match:
            return f"{match.group('amount')}GB"
        console.print("[red]Enter a memory limit in GB, e.g. 8GB.[/red]")


def _prompt_int(label: str, default: int) -> int:
    while True:
        raw = console.input(f"{label} [{default}]: ", markup=False).strip()
        if not raw:
            return default
        try:
            value = int(raw)
        except ValueError:
            console.print("[red]Please enter a whole number.[/red]")
            continue
        if value < 1:
            console.print("[red]Value must be >= 1.[/red]")
            continue
        return value


def _confirm(label: str, default_yes: bool = True) -> bool:
    default = "Y/n" if default_yes else "y/N"
    raw = console.input(f"{label} [{default}]: ", markup=False).strip().lower()
    if not raw:
        return default_yes
    return raw in {"y", "yes"}


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="ukam-os-setup",
        description="Interactive setup wizard for OS builder config.",
    )
    parser.add_argument(
        "--config-out",
        default="config.yaml",
        help="Path to write config YAML (default: config.yaml).",
    )
    parser.add_argument(
        "--env-out",
        default=".env",
        help="Path to write .env template (default: .env).",
    )
    parser.add_argument(
        "--overwrite-env",
        action="store_true",
        help="Overwrite .env output file if it already exists.",
    )
    parser.add_argument(
        "--api-key",
        default=None,
        help="Optional OS API key to write into .env.",
    )
    parser.add_argument(
        "--api-secret",
        default=None,
        help="Optional OS API secret to write into .env.",
    )
    parser.add_argument(
        "--env-example-out",
        dest="env_out",
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--non-interactive",
        action="store_true",
        help="Write config using defaults and any provided required flags.",
    )
    parser.add_argument(
        "--source",
        choices=["ngd", "abp"],
        default=None,
        help="Source dataset type (required with --non-interactive).",
    )
    parser.add_argument("--package-id", help="OS package ID (required in non-interactive mode).")
    parser.add_argument("--version-id", help="OS version ID (required in non-interactive mode).")
    return parser


def main(argv: list[str] | None = None) -> int:
    """Entry point for `ukam-os-setup`."""
    parser = _build_parser()
    args = parser.parse_args(argv)

    config_out = Path(args.config_out).resolve()
    env_out = Path(args.env_out).resolve()
    write_env = True
    overwrite_env = args.overwrite_env
    api_key = args.api_key
    api_secret = args.api_secret

    if (api_key and not api_secret) or (api_secret and not api_key):
        parser.error("--api-key and --api-secret must be provided together")

    config = load_existing_defaults(config_out)

    if args.non_interactive:
        if not args.source:
            parser.error("--source is required with --non-interactive")
        if not args.package_id or not args.version_id:
            parser.error("--package-id and --version-id are required with --non-interactive")

        config["source"]["type"] = args.source
        config["os_downloads"]["package_id"] = args.package_id
        config["os_downloads"]["version_id"] = args.version_id
        config["paths"] = {
            "work_dir": str(config.get("paths", {}).get("work_dir", "./data")),
        }
    else:
        console.print(
            Panel.fit(
                "[bold]OS builder setup wizard[/bold]\nProvide required values first, then optional tuning.",
                border_style="cyan",
            )
        )
        console.print("[bold]Source options:[/bold]")
        console.print("- [bold]ngd[/bold]: OS NGD Address (National Geographic Database)")
        console.print("- [bold]abp[/bold]: OS AddressBase Premium")
        source_default = str(config.get("source", {}).get("type", "ngd"))
        source_value = _prompt_non_empty("source (ngd/abp)", source_default).lower()
        if source_value not in {"ngd", "abp"}:
            parser.error("source must be 'ngd' or 'abp'")
        config["source"]["type"] = source_value

        console.print("[bold]Mandatory settings[/bold]")
        config["os_downloads"]["package_id"] = _prompt_non_empty(
            "OS package_id",
            "",
        )
        config["os_downloads"]["version_id"] = _prompt_non_empty(
            "OS version_id",
            "",
        )

        console.print("\n[bold]Paths[/bold]")
        work_dir = _prompt_non_empty(
            "Where should the tool store its working data?",
            str(config.get("paths", {}).get("work_dir", "./data")),
        )
        config["paths"] = {"work_dir": work_dir}

        if _confirm("Configure advanced processing settings?", default_yes=False):
            config["processing"]["num_chunks"] = _prompt_int(
                "num_chunks",
                int(config["processing"].get("num_chunks", 10)),
            )
            config["processing"]["parquet_compression"] = _prompt_non_empty(
                "parquet_compression",
                str(config["processing"].get("parquet_compression", "zstd")),
            )
            config["processing"]["parquet_compression_level"] = _prompt_int(
                "parquet_compression_level",
                int(config["processing"].get("parquet_compression_level", 9)),
            )
            memory_limit = _prompt_memory_limit(
                "duckdb_memory_limit (optional, e.g. 8GB)",
                str(config["processing"].get("duckdb_memory_limit", "")),
            )
            if memory_limit:
                config["processing"]["duckdb_memory_limit"] = memory_limit
            elif "duckdb_memory_limit" in config["processing"]:
                del config["processing"]["duckdb_memory_limit"]

        if _confirm("Set up .env credentials now?", default_yes=True):
            if env_out.exists() and not overwrite_env:
                if _confirm(
                    f".env already exists at {env_out}. Overwrite with new credentials?",
                    default_yes=False,
                ):
                    overwrite_env = True
                else:
                    write_env = False

            if write_env:
                api_key = _prompt_non_empty("OS_PROJECT_API_KEY")
                api_secret = _prompt_non_empty("OS_PROJECT_API_SECRET")
        else:
            write_env = False

    config_out, env_out, env_written = write_config_and_env(
        config=config,
        config_out=config_out,
        env_out=env_out,
        overwrite_env=overwrite_env,
        write_env=write_env,
        api_key=api_key,
        api_secret=api_secret,
    )

    console.print(f"[green]✓[/green] Wrote config: [bold]{config_out}[/bold]")
    if env_written:
        console.print(f"[green]✓[/green] Wrote .env file: [bold]{env_out}[/bold]")
    elif write_env:
        console.print(
            f"[yellow]•[/yellow] Kept existing .env file: [bold]{env_out}[/bold] "
            "(use --overwrite-env to replace)"
        )
    else:
        console.print(
            f"[yellow]•[/yellow] Skipped .env updates: [bold]{env_out}[/bold] "
            "(existing file left unchanged)"
        )
        console.print(
            "[yellow]Next:[/yellow] add real values for OS_PROJECT_API_KEY and "
            "OS_PROJECT_API_SECRET in .env before running."
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
