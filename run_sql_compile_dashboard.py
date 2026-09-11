from __future__ import annotations

import argparse
import os
import sys
from ipaddress import ip_address
from pathlib import Path
from urllib.parse import urlsplit

from streamlit.web import cli as stcli

from src.sqlCompile_host import host_urls, load_host_config


def print_sharing_addresses(address: str, port: int) -> None:
    office_links = []
    local_links = []
    has_public_address = False
    for url in host_urls(address, port):
        hostname = urlsplit(url).hostname or ""
        try:
            parsed_address = ip_address(hostname)
            local_only = parsed_address.is_loopback
            has_public_address = has_public_address or parsed_address.is_global
        except ValueError:
            local_only = hostname.lower() == "localhost"
        (local_links if local_only else office_links).append(url)
    for url in local_links:
        print(f"THIS COMPUTER ONLY (do not send to coworkers): {url}")
    if office_links:
        print("OFFICE LINK CANDIDATES - test one from a coworker's computer:")
        for url in office_links:
            print(f"  {url}")
        print("Coworkers need only a browser. Keep this host running and awake.")
        print("If none opens remotely, ask IT to check the host's inbound TCP port and office network access.")
        if has_public_address:
            print("A listed IP address is publicly routable. Have IT restrict access to approved office computers before sharing student records.")
    else:
        print("No office link is available from this configuration.")
        print('For office sharing, set address to "0.0.0.0" in config/sqlCompile_host.json and check the network connection.')
    print("Shared-drive access alone does not guarantee browser access to this host.")


def main() -> None:
    root = Path(__file__).resolve().parent
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))
    target = root / "app" / "sql_compile_dashboard.py"
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--shared", action="store_true")
    parser.add_argument("--host-config")
    options, streamlit_args = parser.parse_known_args()
    if options.host_config and not options.shared:
        parser.error("--host-config requires --shared")
    if options.shared:
        if options.host_config and not Path(options.host_config).is_absolute():
            options.host_config = str(root / options.host_config)
        try:
            config = load_host_config(options.host_config)
            if not config.database.is_file():
                raise FileNotFoundError(f"Compiled database not found: {config.database}. Compile or copy your data on this host first.")
        except (OSError, ValueError) as exc:
            parser.error(str(exc))
        # These settings belong to the host config, not an unvalidated second set of flags.
        protected = ("--server.address", "--server.port", "--server.enableCORS", "--server.enableXsrfProtection")
        if any(argument.split("=", 1)[0] in protected for argument in streamlit_args):
            parser.error("In shared mode, set address/port in --host-config. CORS and XSRF protection remain enabled.")
        os.environ["FSL_DASHBOARD_SHARED"] = "1"
        if options.host_config:
            os.environ["FSL_DASHBOARD_HOST_CONFIG"] = str(Path(options.host_config).resolve())
        streamlit_args = [
            "--server.address", config.address, "--server.port", str(config.port),
            "--server.headless", "true", "--server.enableCORS", "true",
            "--server.enableXsrfProtection", "true", "--browser.gatherUsageStats", "false",
            "--theme.base", "light", "--client.toolbarMode", "viewer",
            *streamlit_args,
        ]
        print("Shared dashboard: no sign-in. Everyone with network access can view and edit student records.")
        print("Use only on an approved, trusted network. Do not expose this port to the public internet.")
        print_sharing_addresses(config.address, config.port)
        for path in config.data_paths[1:]:
            if not path.exists():
                print(f"Missing saved-data file: {path}. Copy existing decisions/exceptions before sharing.")
    else:
        os.environ.pop("FSL_DASHBOARD_SHARED", None)
    sys.argv = ["streamlit", "run", str(target), *streamlit_args]
    raise SystemExit(stcli.main())


if __name__ == "__main__":
    main()
