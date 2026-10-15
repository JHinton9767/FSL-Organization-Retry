"""Back up the office baseline and run publication checks without publishing."""
from __future__ import annotations

import argparse
import json
import shutil
import sqlite3
from contextlib import ExitStack, closing
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from src.path_config import ROOT
from src.sqlCompile import sqlCompile
from src.sqlCompile_audit import audit_path
from src.sqlCompile_host import load_host_config
from src.sqlCompile_records import METRIC_VERSION
from src.sqlCompile_sources import file_digest
from src.sqlCompile_storage import data_lock, read_database, atomic_write_text
from src.sqlCompile_viewer import publish_viewer, DEFAULT_VIEWER_PATH


def backup_baseline(config, *, extra_paths=()) -> Path:
    if not config.database.exists():
        raise FileNotFoundError("No office database is available to back up. Configure or restore the office data first.")
    folder = config.database.parent / "baselines" / (datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ") + "_" + uuid4().hex[:8])
    paths = list(dict.fromkeys([*config.data_paths, config.reporting_settings, *extra_paths]))
    manifest = {"created_at": datetime.now(timezone.utc).isoformat(), "metric_version": METRIC_VERSION, "files": []}
    with ExitStack() as stack:
        for path in sorted(paths, key=lambda p: str(p.resolve()).casefold()):
            stack.enter_context(data_lock(path))
        folder.mkdir(parents=True)
        for number, path in enumerate(paths):
            if not path.exists():
                manifest["files"].append({"original": str(path.resolve()), "missing": True})
                continue
            target = folder / f"{number:02}_{path.name}"
            if path == config.database:
                with read_database(path) as source, closing(sqlite3.connect(target)) as destination:
                    source.backup(destination)
            else:
                shutil.copy2(path, target)
            manifest["files"].append({"original": str(path.resolve()), "backup": target.name, "sha256": file_digest(target)})
            history = audit_path(path)
            if history.exists():
                target_history = folder / f"{number:02}_{path.name}.history.sqlite"
                with closing(sqlite3.connect(history.resolve().as_uri() + "?mode=ro", uri=True)) as source, closing(sqlite3.connect(target_history)) as destination:
                    source.backup(destination)
                manifest["files"].append({"original": str(history.resolve()), "backup": target_history.name, "sha256": file_digest(target_history)})
        atomic_write_text(folder / "manifest.json", json.dumps(manifest, indent=2) + "\n")
    return folder


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host-config", type=Path)
    parser.add_argument("--path-config", type=Path, help="Optional source-folder configuration used for recompilation.")
    parser.add_argument("--recompile", action="store_true", help="After backing up, import changed rosters and rebuild the owner database.")
    parser.add_argument("--viewer", type=Path, default=DEFAULT_VIEWER_PATH, help="Existing viewer to compare; it will not be replaced.")
    args = parser.parse_args(argv)
    try:
        config = load_host_config(args.host_config)
        extra = [args.path_config or ROOT / "config" / "local_paths.yaml", args.viewer]
        if args.host_config:
            extra.append(args.host_config)
        elif (ROOT / "config" / "sqlCompile_host.json").exists():
            extra.append(ROOT / "config" / "sqlCompile_host.json")
        backup = backup_baseline(config, extra_paths=extra)
        print(f"Baseline backup: {backup}")
        if args.recompile:
            result = sqlCompile(output_path=config.database, config_path=args.path_config)
            print(f"Compiled {result.row_count:,} rows from {result.source_file_count:,} files. Issues: {result.issue_count:,}.")
        def review(report):
            print(f"Owner's quality report: {report['report_file']}")
            for level in ("errors", "warnings"):
                for message in report[level]:
                    print(f"{level.upper()}: {message}")
            return False
        publish_viewer(config, args.viewer, check_only=True, review_check=review)
    except (OSError, ValueError, RuntimeError) as exc:
        print(f"Verification needs attention: {exc}")
        return 1
    print("Verification completed. Review any warnings before publishing. The shared dashboard was not replaced.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
