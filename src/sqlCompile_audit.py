"""Permanent, private history of review-file changes, separate from rotating backups."""
from __future__ import annotations

import getpass
import hashlib
import sqlite3
from contextlib import closing, contextmanager
from contextvars import ContextVar
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

import pandas as pd


_reviewer = ContextVar("sql_compile_reviewer", default=None)


@contextmanager
def review_context(reviewer: str, reason: str = "", evidence: str = ""):
    token = _reviewer.set({"reviewer": reviewer.strip(), "reason": reason.strip(), "evidence": evidence.strip()})
    try:
        yield
    finally:
        _reviewer.reset(token)


def audit_path(path: Path) -> Path:
    return path.parent / "_history" / (path.name + ".sqlite")


def digest(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def begin_change(path: Path, before: bytes | None, after: bytes) -> str:
    """Write ahead of the CSV replacement: a failed save cannot lose the old decision."""
    destination = audit_path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    context = _reviewer.get() or {"reviewer": getpass.getuser(), "reason": "", "evidence": ""}
    change_id = uuid4().hex
    with closing(sqlite3.connect(destination)) as connection, connection:
        connection.execute("""CREATE TABLE IF NOT EXISTS changes (
            id TEXT PRIMARY KEY, recorded_at TEXT, reviewer TEXT, reason TEXT, evidence TEXT,
            before_hash TEXT, after_hash TEXT, before_csv BLOB, after_csv BLOB, state TEXT)""")
        connection.execute("INSERT INTO changes VALUES (?,?,?,?,?,?,?,?,?,?)", (
            change_id, datetime.now(timezone.utc).isoformat(), context["reviewer"],
            context["reason"], context["evidence"], digest(before) if before is not None else "",
            digest(after), before, after, "pending",
        ))
    return change_id


def finish_change(path: Path, change_id: str, state: str) -> None:
    with closing(sqlite3.connect(audit_path(path))) as connection, connection:
        connection.execute("UPDATE changes SET state=? WHERE id=?", (state, change_id))


def read_change_history(path: Path) -> pd.DataFrame:
    destination = audit_path(path)
    columns = ["id", "recorded_at", "reviewer", "reason", "evidence", "before_hash", "after_hash", "state"]
    if not destination.exists():
        return pd.DataFrame(columns=columns)
    with closing(sqlite3.connect(destination.resolve().as_uri() + "?mode=ro", uri=True)) as connection:
        return pd.read_sql_query("SELECT " + ",".join(columns) + " FROM changes ORDER BY recorded_at DESC", connection)


def change_contents(path: Path, change_id: str) -> tuple[bytes, bytes]:
    with closing(sqlite3.connect(audit_path(path).resolve().as_uri() + "?mode=ro", uri=True)) as connection:
        row = connection.execute("SELECT before_csv, after_csv FROM changes WHERE id=?", (change_id,)).fetchone()
    if row is None:
        raise ValueError("Review change not found.")
    return row[0] or b"", row[1] or b""


def audit_warning(path: Path) -> str | None:
    if not path.exists() or not path.stat().st_size:
        return None
    history = read_change_history(path)
    if history.empty:
        return f"{path.name}: pre-existing decisions have no permanent history yet; back up this file before migration."
    current = digest(path.read_bytes())
    latest = history.iloc[0]
    if latest["state"] == "pending":
        return f"{path.name}: interrupted save requires review; both proposed and prior records are retained in history."
    committed = history.loc[history["state"].eq("saved")]
    if not committed.empty and committed.iloc[0]["after_hash"] != current:
        return f"{path.name}: records changed outside the dashboard. Review and save them through the dashboard to establish history."
    return None
