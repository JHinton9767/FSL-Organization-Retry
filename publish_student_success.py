"""Export the aggregate Student Success & Impact report without running a server."""
from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from src.path_config import ROOT
from src.sqlCompile_dashboard import load_dashboard_tables
from src.sqlCompile_host import load_host_config, data_revision
from src.sqlCompile_reporting import read_reporting_cutoff, REPORTING_EXAMPLE
from src.sqlCompile_storage import atomic_write_text, data_lock
from src.sqlCompile_success import build_student_success, render_public_summary, SUCCESS_VERSION
from src.sqlCompile_success_records import DEGREE_LEVELS


DEFAULT_OUTPUT = ROOT / "output" / "sqlCompile" / "viewer" / "FSL_Student_Success_Impact.html"


def publish_student_success(config, output=DEFAULT_OUTPUT, **filters):
    output = Path(output).resolve()
    sources = (*config.data_paths, config.reporting_settings, REPORTING_EXAMPLE)
    if output.suffix.lower() != ".html" or output in {p.resolve() for p in sources}:
        raise ValueError("Choose an HTML destination that does not replace a source record.")
    revision, previous = data_revision(sources), data_revision((output,))
    cutoff = read_reporting_cutoff(config.reporting_settings)
    tables = load_dashboard_tables(database_path=config.database, manual_status_file=config.manual_status,
        duplicate_name_resolution_file=config.name_choices, duplicate_name_recheck_file=config.name_rechecks,
        zero_member_periods_file=config.zero_member_periods, reporting_cutoff=cutoff, create_review_files=False,
        university_records_file=config.university_records, graduation_records_file=config.graduation_records,
        academic_records_file=config.academic_records, impact_records_file=config.impact_records)
    report = build_student_success(tables, **filters)
    summary = report["public"]
    html = render_public_summary(summary)
    with data_lock(output):
        if data_revision(sources) != revision or data_revision((output,)) != previous:
            raise RuntimeError("Records or the previous report changed during export. No report was replaced; refresh and retry.")
        archive = config.database.parent / "success_publications" / (datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ") + "_" + uuid4().hex[:8])
        if output.exists():
            atomic_write_text(archive / "previous.html", output.read_text(encoding="utf-8-sig"))
        atomic_write_text(archive / "candidate.html", html)
        atomic_write_text(output, html)
        atomic_write_text(archive / "published.json", json.dumps({"generated": summary["generated"], "version": SUCCESS_VERSION,
            "destination": str(output), "period": summary["period"], "ready_metrics": [c["key"] for c in summary["cards"] if c["status"] == "Ready"],
            "withheld_metrics": [c["key"] for c in summary["cards"] if c["status"] != "Ready"],
            "html_sha256": hashlib.sha256(html.encode()).hexdigest(), "input_revisions": revision}, indent=2) + "\n")
    return output, summary


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host-config", type=Path)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--start", help="Award/activity period start, e.g. Fall 2025")
    parser.add_argument("--end", help="Report end, at or before the confirmed reporting cutoff")
    parser.add_argument("--cohort", action="append", help="Joining cohort; repeat to select several")
    parser.add_argument("--chapter", action="append", help="Joining chapter; repeat to select several")
    parser.add_argument("--degree-level", choices=["All degree levels", *DEGREE_LEVELS], default="Bachelor's")
    parser.add_argument("--minimum-group", type=int, default=5)
    args = parser.parse_args(argv)
    try:
        output, summary = publish_student_success(load_host_config(args.host_config), args.output,
            report_start=args.start, report_end=args.end, semesters=args.cohort, chapters=args.chapter,
            degree_level=args.degree_level, minimum_group=args.minimum_group)
    except (OSError, ValueError, RuntimeError) as exc:
        print(f"Student success export needs attention: {exc}")
        return 1
    ready = sum(c["status"] == "Ready" for c in summary["cards"])
    print(f"External report: {output}")
    print(f"Period: {summary['period']}; {ready} verified headline metric(s) ready.")
    print("Incomplete rates and small student groups are withheld. Review the period, population, and methodology before sharing.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
