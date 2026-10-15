"""Versioned source observations and reusable parsing results in the owner database."""
from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from src.sqlCompile_storage import read_database

PARSER_VERSION = "2026-10-15.1"
SOURCE_VERSIONS = "sqlCompile_source_versions"
SOURCE_MANIFEST = "sqlCompile_source_manifest"
SOURCE_OBSERVATIONS = "sqlCompile_source_observations"
MANIFEST_COLUMNS = ["Source Path", "Source File", "SHA256", "Parser Version", "Imported At", "Student Rows", "Issue Count", "Reused"]


def file_digest(path: Path) -> str:
    result = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            result.update(block)
    return result.hexdigest()


def read_optional_table(database: Path, table: str) -> pd.DataFrame:
    if not database.exists():
        return pd.DataFrame()
    with read_database(database) as connection:
        if not connection.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone():
            return pd.DataFrame()
        return pd.read_sql_query('SELECT * FROM "' + table.replace('"', '""') + '"', connection)


def source_cache(database: Path | None) -> tuple[dict, set[str]]:
    if database is None or not database.exists():
        return {}, set()
    versions = read_optional_table(database, SOURCE_VERSIONS)
    manifest = read_optional_table(database, SOURCE_MANIFEST)
    cache = {}
    for row in versions.to_dict("records"):
        if row["parser_version"] == PARSER_VERSION:
            cache[(row["source_path"], row["sha256"], row["source_file"])] = row
    return cache, set(manifest.get("Source Path", []))


def persist_sources(connection, rows: pd.DataFrame) -> None:
    # Current observations are replaceable; every parsed file version remains in the archive.
    connection.execute(f"""CREATE TABLE IF NOT EXISTS {SOURCE_VERSIONS} (
        source_path TEXT, sha256 TEXT, source_file TEXT, parser_version TEXT, imported_at TEXT,
        rows_json TEXT, issues_json TEXT, PRIMARY KEY (source_path, sha256, source_file, parser_version))""")
    for record in rows.attrs.get("source_versions", []):
        connection.execute(f"INSERT OR IGNORE INTO {SOURCE_VERSIONS} VALUES (?,?,?,?,?,?,?)", record)
    manifest = pd.DataFrame(rows.attrs.get("source_manifest", []), columns=MANIFEST_COLUMNS)
    manifest.to_sql(SOURCE_MANIFEST, connection, if_exists="replace", index=False)
    observations = rows.copy()
    if observations.empty:
        observations = pd.DataFrame(columns=["Semester", "Chapter", "Student ID", "Student Name", "Status", "_source_file", "_source_sheet", "_source_row_index", "_source_path", "_source_sha256", "_imported_at"])
    observations.to_sql(SOURCE_OBSERVATIONS, connection, if_exists="replace", index=False)


def source_version(path: Path, label: str, sha: str, rows: list[dict], issues: list[dict]) -> tuple:
    return (str(path.resolve()), sha, label, PARSER_VERSION, datetime.now(timezone.utc).isoformat(),
            json.dumps(rows, ensure_ascii=False), json.dumps(issues, ensure_ascii=False))
