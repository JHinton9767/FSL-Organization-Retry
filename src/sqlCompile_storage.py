from __future__ import annotations

import os
import shutil
import sqlite3
import tempfile
from contextlib import closing, contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Iterator, Sequence
from uuid import uuid4

import pandas as pd
from filelock import FileLock


BACKUP_LIMIT = 10
Normalizer = Callable[[pd.DataFrame], pd.DataFrame]


class ReviewConflictError(OSError):
    """The saved records changed after the reviewer loaded them."""


def data_lock(path: str | Path) -> FileLock:
    destination = Path(path).resolve()
    destination.parent.mkdir(parents=True, exist_ok=True)
    return FileLock(os.path.normcase(str(destination)) + ".lock", timeout=15, is_singleton=True)


def _backup_path(path: Path) -> Path:
    folder = path.parent / "_backups" / path.name
    folder.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    return folder / f"{timestamp}_{uuid4().hex[:8]}{path.suffix}"


def _prune_backups(path: Path) -> None:
    folder = path.parent / "_backups" / path.name
    for backup in sorted(folder.glob(f"*{path.suffix}"), reverse=True)[BACKUP_LIMIT:]:
        try:
            backup.unlink()
        except OSError:
            # A backup in use should not turn a completed save into an error.
            continue


def _write_csv(frame: pd.DataFrame, path: Path) -> None:
    content = frame.to_csv(index=False).encode("utf-8")
    if path.exists():
        if path.read_bytes() == content:
            return
        shutil.copy2(path, _backup_path(path))
    temporary: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(dir=path.parent, suffix=".tmp", delete=False) as handle:
            temporary = Path(handle.name)
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)
    _prune_backups(path)


def _read_csv(path: Path, columns: Sequence[str]) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(columns=columns)
    try:
        return pd.read_csv(path, dtype=str).fillna("")
    except pd.errors.EmptyDataError:
        return pd.DataFrame(columns=columns)


def read_review_csv(path: Path, columns: Sequence[str], *, create: bool = True) -> pd.DataFrame:
    if not create and not path.exists():
        return pd.DataFrame(columns=columns)
    with data_lock(path):
        if create and not path.exists():
            _write_csv(pd.DataFrame(columns=columns), path)
        return _read_csv(path, columns)


def write_review_csv(
    frame: pd.DataFrame,
    path: Path,
    normalize: Normalizer,
    *,
    expected_rows: pd.DataFrame | None = None,
) -> Path:
    prepared = normalize(frame)
    with data_lock(path):
        current = normalize(_read_csv(path, prepared.columns))
        if expected_rows is not None:
            expected = normalize(expected_rows).reset_index(drop=True)
            if not current.reset_index(drop=True).equals(expected) and not current.reset_index(drop=True).equals(prepared.reset_index(drop=True)):
                raise ReviewConflictError("Saved records changed in another session. Refresh Dashboard Data before saving again.")
        _write_csv(prepared, path)
    return path


def append_review_csv(
    incoming: pd.DataFrame,
    path: Path,
    normalize: Normalizer,
    key_columns: Sequence[str],
    *,
    expected_rows: pd.DataFrame | None = None,
) -> tuple[Path, int]:
    incoming = normalize(incoming)
    with data_lock(path):
        current = normalize(_read_csv(path, incoming.columns))
        if expected_rows is not None and not incoming.empty:
            def row_lookup(frame: pd.DataFrame) -> dict[tuple, tuple]:
                unique = frame.drop_duplicates(subset=list(key_columns), keep="last")
                keys = unique.loc[:, list(key_columns)].itertuples(index=False, name=None)
                return dict(zip(keys, unique.itertuples(index=False, name=None)))

            before = row_lookup(normalize(expected_rows))
            now = row_lookup(current)
            proposed = row_lookup(incoming)
            conflicts = [key for key, row in proposed.items() if now.get(key) != before.get(key) and now.get(key) != row]
            if conflicts:
                raise ReviewConflictError(
                    f"{len(conflicts)} record(s) changed in another session. No rows were saved. "
                    "Refresh Dashboard Data and review those records before saving again."
                )
        if incoming.empty:
            if not path.exists():
                _write_csv(current, path)
            return path, 0
        combined = pd.concat([current, incoming], ignore_index=True)
        combined = combined.drop_duplicates(subset=list(key_columns), keep="last")
        _write_csv(combined, path)
    return path, len(incoming)


@contextmanager
def read_database(path: Path) -> Iterator[sqlite3.Connection]:
    path = path.resolve()
    with data_lock(path), closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)) as connection:
        yield connection


@contextmanager
def atomic_database_update(path: Path) -> Iterator[sqlite3.Connection]:
    """Publish a complete database while retaining tables owned by other workflows."""
    path = path.resolve()
    with data_lock(path):
        handle, name = tempfile.mkstemp(dir=path.parent, suffix=".sqlite.tmp")
        os.close(handle)
        temporary = Path(name)
        try:
            if path.exists():
                backup = _backup_path(path)
                with read_database(path) as source, closing(sqlite3.connect(backup)) as destination:
                    source.backup(destination)
                shutil.copy2(backup, temporary)
            with closing(sqlite3.connect(temporary)) as connection, connection:
                yield connection
            os.replace(temporary, path)
        finally:
            temporary.unlink(missing_ok=True)
        _prune_backups(path)
