from __future__ import annotations

import argparse
import json
import os
import re
import tempfile
from datetime import datetime, timezone
from pathlib import Path

from plotly.offline import get_plotlyjs

from app.charts import PERSISTENCE_CHART_OUTCOME_ORDER, PERSISTENCE_OUTCOME_COLORS
from src.path_config import ROOT
from src.sqlCompile_cohort import _semester_sort
from src.sqlCompile_dashboard import SqlCompileDashboardTables, build_sql_compile_milestone_dashboard, load_dashboard_tables
from src.sqlCompile_host import HostConfig, data_revision, load_host_config


ASSETS = ROOT / "app" / "viewer"
DEFAULT_VIEWER_PATH = ROOT / "output" / "sqlCompile" / "viewer" / "FSL_Dashboard.html"


def build_viewer_payload(tables: SqlCompileDashboardTables) -> dict:
    chapters = sorted({str(value).strip() for value in tables.outcomes["Cohort Chapter"].dropna() if str(value).strip()})
    dashboard = build_sql_compile_milestone_dashboard(
        tables.timeline, tables.outcomes, tables.selected_semesters,
        selected_chapters=chapters, chart_milestone_offsets=[1, 2, 3, 4, 5, 6],
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
    latest = max(tables.timeline["Semester"].dropna().astype(str), key=_semester_sort, default="")
    return {
        "schema": 1,
        "published": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "dataThrough": latest,
        "semesters": semesters,
        "chapters": sorted({row[1] for row in units}, key=str.casefold),
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


def publish_viewer(config: HostConfig, output: Path = DEFAULT_VIEWER_PATH) -> tuple[Path, int]:
    output = output.resolve()
    if output.suffix.lower() != ".html":
        raise ValueError("The viewer destination must end in .html.")
    if output in {path.resolve() for path in config.data_paths}:
        raise ValueError("The viewer cannot replace a source data file.")
    for path in (config.database, config.manual_status, config.zero_member_periods):
        if not path.is_file():
            raise FileNotFoundError(
                f"Required source file is missing: {path}. Restore existing data/configuration before publishing. "
                "See docs/readonly_viewer_setup.md."
            )
    revision = data_revision(config.data_paths)
    tables = load_dashboard_tables(
        database_path=config.database, manual_status_file=config.manual_status,
        duplicate_name_resolution_file=config.name_choices, duplicate_name_recheck_file=config.name_rechecks,
        zero_member_periods_file=config.zero_member_periods, create_review_files=False,
    )
    payload = build_viewer_payload(tables)
    html = render_viewer(payload)
    if data_revision(config.data_paths) != revision:
        raise RuntimeError("Source records changed during publication. No viewer was replaced. Finish saving and publish again.")
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=output.parent, suffix=".tmp", delete=False) as handle:
            temporary = Path(handle.name)
            handle.write(html)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, output)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)
    return output, sum(row[4] for row in payload["units"] if row[2] == 1)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Publish a self-contained, read-only P&G dashboard. No server or client installation required.")
    parser.add_argument("--host-config", help="Optional existing sqlCompile host JSON; uses only its source data paths.")
    parser.add_argument("--output", type=Path, default=DEFAULT_VIEWER_PATH, help="Destination HTML file, optionally on the approved shared SSD.")
    args = parser.parse_args(argv)
    try:
        destination, students = publish_viewer(load_host_config(args.host_config), args.output)
    except (OSError, ValueError, RuntimeError) as exc:
        parser.exit(1, f"Could not publish the viewer: {exc}\n")
    print(f"Read-only dashboard published: {destination}")
    print(f"Cohort students: {students:,}")
    print("Coworkers open this HTML file in their browser. No Python, SQL installation, sign-in, or server is needed.")
    print("This is a published snapshot. Publish again after corrections or roster updates; coworkers then reopen the file.")
    print("The file contains aggregate counts, not student IDs, names, or manual notes. Keep it on approved office storage.")
    return 0
