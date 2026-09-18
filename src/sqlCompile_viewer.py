from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

from plotly.offline import get_plotlyjs

from app.charts import PERSISTENCE_CHART_OUTCOME_ORDER, PERSISTENCE_OUTCOME_COLORS
from src.path_config import ROOT
from src.sqlCompile_cohort import _semester_sort, read_sql_compile_table
from src.sqlCompile_dashboard import SqlCompileDashboardTables, build_sql_compile_milestone_dashboard, load_dashboard_tables
from src.sqlCompile_host import HostConfig, data_revision, load_host_config
from src.sqlCompile_councils import COUNCILS, council_for_chapter
from src.sqlCompile_publication import build_publication_check, read_previous_publication
from src.sqlCompile_reporting import REPORTING_EXAMPLE, read_reporting_cutoff, save_reporting_cutoff, validate_reporting_cutoff
from src.sqlCompile_storage import atomic_write_text, data_lock


ASSETS = ROOT / "app" / "viewer"
DEFAULT_VIEWER_PATH = ROOT / "output" / "sqlCompile" / "viewer" / "FSL_Dashboard.html"


def build_viewer_payload(tables: SqlCompileDashboardTables) -> dict:
    chapters = sorted({str(value).strip() for value in tables.outcomes["Cohort Chapter"].dropna() if str(value).strip()})
    dashboard = build_sql_compile_milestone_dashboard(
        tables.timeline, tables.outcomes, tables.selected_semesters,
        selected_chapters=chapters, chart_milestone_offsets=[1, 2, 3, 4, 5, 6],
        reporting_cutoff=getattr(tables, "reporting_cutoff", None),
    )
    details = dashboard["detail_frame"]
    if details.empty:
        raise ValueError("No new-member cohorts are available to publish. Compile the complete roster database first.")
    # Only aggregate counts leave the publisher; no student-level data is embedded.
    columns = ["Cohort Semester", "Cohort Chapter", "Milestone Sort", "P&G Outcome Bucket"]
    totals = details.groupby(columns, dropna=False, sort=False).size().reset_index(name="Count")
    units = [
        [str(semester), str(chapter), int(year), str(outcome), int(count)]
        for semester, chapter, year, outcome, count in totals.itertuples(index=False, name=None)
    ]
    semesters = sorted({row[0] for row in units}, key=_semester_sort)
    latest = getattr(tables, "reporting_cutoff", None) or max(tables.timeline["Semester"].dropna().astype(str), key=_semester_sort, default="")
    return {
        "schema": 1,
        "published": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "dataThrough": latest,
        "semesters": semesters,
        "chapters": sorted({row[1] for row in units}, key=str.casefold),
        "councils": list(COUNCILS),
        "chapterCouncils": {chapter: council_for_chapter(chapter) for chapter in sorted({row[1] for row in units})},
        "outcomes": PERSISTENCE_CHART_OUTCOME_ORDER,
        "colors": PERSISTENCE_OUTCOME_COLORS,
        "units": units,
    }


def render_viewer(payload: dict) -> str:
    serialized = json.dumps(payload, ensure_ascii=True, allow_nan=False, separators=(",", ":"))
    serialized = serialized.replace("<", "\\u003c").replace(">", "\\u003e").replace("&", "\\u0026")
    template = (ASSETS / "index.html").read_text(encoding="utf-8")
    parts = {
        "/*VIEWER_CSS*/": (ASSETS / "viewer.css").read_text(encoding="utf-8"),
        "/*PLOTLY_JS*/": get_plotlyjs(),
        "/*VIEWER_MODEL*/": (ASSETS / "model.js").read_text(encoding="utf-8"),
        "/*VIEWER_UI*/": (ASSETS / "viewer.js").read_text(encoding="utf-8"),
        "/*VIEWER_DATA*/": serialized,
    }
    for marker in parts:
        if template.count(marker) != 1:
            raise ValueError(f"Invalid viewer template marker: {marker}")
    return re.sub("|".join(re.escape(marker) for marker in parts), lambda match: parts[match.group()], template)


def publish_viewer(
    config: HostConfig, output: Path = DEFAULT_VIEWER_PATH, *, allow_warnings: bool = False,
    check_only: bool = False, review_check: Callable[[dict], bool] | None = None,
) -> tuple[Path, int]:
    output = output.resolve()
    if output.suffix.lower() != ".html":
        raise ValueError("The viewer destination must end in .html.")
    source_paths = (*config.data_paths, config.reporting_settings, REPORTING_EXAMPLE)
    if output in {path.resolve() for path in source_paths}:
        raise ValueError("The viewer cannot replace a source data file.")
    for path in (config.database, config.manual_status, config.zero_member_periods):
        if not path.is_file():
            raise FileNotFoundError(
                f"Required source file is missing: {path}. Restore existing data/configuration before publishing. "
                "See docs/readonly_viewer_setup.md."
            )
    revision = data_revision(source_paths)
    previous_revision = data_revision((output,))
    cutoff = read_reporting_cutoff(config.reporting_settings)
    tables = load_dashboard_tables(
        database_path=config.database, manual_status_file=config.manual_status,
        duplicate_name_resolution_file=config.name_choices, duplicate_name_recheck_file=config.name_rechecks,
        zero_member_periods_file=config.zero_member_periods, create_review_files=False, reporting_cutoff=cutoff,
    )
    payload = build_viewer_payload(tables)
    previous_error = None
    try:
        previous = read_previous_publication(output)
    except ValueError as exc:
        previous, previous_error = None, str(exc)
    report = build_publication_check(config, tables, payload, previous)
    if previous_error:
        report["warnings"].append(f"Previous publication cannot be compared: {previous_error}")
    report_path = config.database.parent / "publication_checks" / f"{output.stem}.json"
    report["report_file"] = str(report_path.resolve())
    atomic_write_text(report_path, json.dumps(report, indent=2, allow_nan=False) + "\n")
    approved = review_check(report) if review_check is not None else False
    if data_revision(source_paths) != revision:
        raise RuntimeError("Source records changed during publication. No viewer was replaced. Finish saving and publish again.")
    if check_only:
        return output, report["cohort_students"]
    if report["warnings"] and not (allow_warnings or approved):
        raise ValueError(f"Publication paused for review ({len(report['warnings'])} warnings). Prior viewer is unchanged. Review {report_path}; use --allow-warnings only after reviewing them.")
    html = render_viewer(payload)
    with data_lock(output):
        if data_revision(source_paths) != revision or data_revision((output,)) != previous_revision:
            raise RuntimeError("Source records or the published viewer changed. No viewer was replaced; publish again.")
        atomic_write_text(output, html)
    return output, sum(row[4] for row in payload["units"] if row[2] == 1)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Publish a self-contained, read-only P&G dashboard. No server or client installation required.")
    parser.add_argument("--host-config", help="Optional existing sqlCompile host JSON; uses only its source data paths.")
    parser.add_argument("--output", type=Path, default=DEFAULT_VIEWER_PATH, help="Destination HTML file, optionally on the approved shared SSD.")
    parser.add_argument("--cutoff", help="Save the owner-confirmed latest complete roster semester, for example Spring 2026.")
    parser.add_argument("--check-only", action="store_true", help="Write the owner's quality check without replacing the office viewer.")
    parser.add_argument("--allow-warnings", action="store_true", help="Acknowledge reviewed quality warnings for this publication; validation errors still stop publication.")
    args = parser.parse_args(argv)

    def review(report: dict) -> bool:
        print(f"Data complete through: {report['reporting_cutoff']}")
        print(f"Cohort students: {report['cohort_students']:,}")
        print(f"Later roster rows held back: {report['deferred_roster_rows']:,}; later manual rows: {report['deferred_manual_rows']:,}")
        print(f"Owner's quality report: {report['report_file']}")
        if not report["comparison_available"]:
            print("No comparable previous publication; change detection will start with the next publication.")
        for warning in report["warnings"]:
            print(f"WARNING: {warning}")
        if not report["warnings"]:
            print("Publication checks passed without warnings.")
        if report["warnings"] and not (args.check_only or args.allow_warnings) and sys.stdin.isatty():
            return input("After reviewing the warnings, publish this snapshot? [y/N] ").strip().lower() in {"y", "yes"}
        return False

    try:
        config = load_host_config(args.host_config)
        if args.cutoff:
            validate_reporting_cutoff(args.cutoff, read_sql_compile_table(config.database))
            save_reporting_cutoff(args.cutoff, config.reporting_settings)
        destination, students = publish_viewer(config, args.output, allow_warnings=args.allow_warnings,
                                              check_only=args.check_only, review_check=review)
    except (OSError, ValueError, RuntimeError) as exc:
        parser.exit(1, f"Could not publish the viewer: {exc}\n")
    if args.check_only:
        print("Quality check complete. The office viewer was not replaced.")
        return 0
    print(f"Read-only dashboard published: {destination}")
    print(f"Cohort students: {students:,}")
    print("Coworkers open this HTML file in their browser. No Python, SQL installation, sign-in, or server is needed.")
    print("This is a published snapshot. Publish again after corrections or roster updates; coworkers then reopen the file.")
    print("The file contains aggregate counts, not student IDs, names, or manual notes. Keep it on approved office storage.")
    return 0
