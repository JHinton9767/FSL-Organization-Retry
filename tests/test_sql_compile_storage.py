from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor

import pandas as pd
import pytest

from src.sqlCompile import OUTPUT_COLUMNS, write_sqlite
from src.sqlCompile_cohort import (
    append_manual_status_rows,
    read_manual_status_rows,
    write_manual_status_rows,
)
from src.sqlCompile_dashboard import (
    append_duplicate_name_recheck_rows,
    append_duplicate_name_resolution_rows,
    read_duplicate_name_recheck_rows,
    read_duplicate_name_resolution_rows,
)
from src.sqlCompile_storage import BACKUP_LIMIT, ReviewConflictError, atomic_database_update, read_database


def manual_row(student_id="0001", status="G"):
    return pd.DataFrame([{
        "Cohort Semester": "Fall 2020", "Cohort Chapter": "Alpha", "Semester": "Spring 2024",
        "Chapter": "Alpha", "Student ID": student_id, "Status": status, "Notes": "Verified",
    }])


def _process_save(args):
    path, student_id = args
    append_manual_status_rows(manual_row(student_id), path)


@pytest.mark.parametrize("executor", [ThreadPoolExecutor, ProcessPoolExecutor])
def test_concurrent_saves_keep_every_student(tmp_path, executor):
    path = tmp_path / "manual.csv"
    with executor(max_workers=3) as pool:
        list(pool.map(_process_save, [(path, f"{i:04}") for i in range(12)]))
    saved = read_manual_status_rows(path)
    assert sorted(saved["Student ID"]) == [f"{i:04}" for i in range(12)]
    assert len(list((tmp_path / "_backups" / path.name).glob("*.csv"))) == BACKUP_LIMIT


def test_stale_batch_rejected_without_partial_save(tmp_path):
    path = tmp_path / "manual.csv"
    original = read_manual_status_rows(path)
    append_manual_status_rows(manual_row(status="RS"), path, expected_rows=original)
    batch = pd.concat([manual_row(), manual_row("0002")])
    with pytest.raises(ReviewConflictError, match="No rows were saved"):
        append_manual_status_rows(batch, path, expected_rows=original)
    assert read_manual_status_rows(path)["Status"].tolist() == ["RS"]
    append_manual_status_rows(manual_row(status="RS"), path, expected_rows=original)
    append_manual_status_rows(manual_row("0002"), path, expected_rows=original)
    assert len(read_manual_status_rows(path)) == 2


def test_stale_whole_file_cannot_erase_new_rows(tmp_path):
    path = tmp_path / "manual.csv"
    original = read_manual_status_rows(path)
    append_manual_status_rows(manual_row(), path)
    with pytest.raises(ReviewConflictError):
        write_manual_status_rows(original, path, expected_rows=original)
    assert len(read_manual_status_rows(path)) == 1


@pytest.mark.parametrize("append,read,column", [
    (append_duplicate_name_resolution_rows, read_duplicate_name_resolution_rows, "Student Name"),
    (append_duplicate_name_recheck_rows, read_duplicate_name_recheck_rows, "Notes"),
])
def test_name_ledgers_protect_stale_decisions(tmp_path, append, read, column):
    path = tmp_path / "names.csv"
    original = read(path)
    append(pd.DataFrame([{"Student ID": "0001", column: "First"}]), path, expected_rows=original)
    with pytest.raises(ReviewConflictError):
        append(pd.DataFrame([{"Student ID": "0001", column: "Second"}]), path, expected_rows=original)
    assert read(path).iloc[0][column] == "First"


def test_failed_csv_replacement_keeps_original_and_backup(tmp_path, monkeypatch):
    from src import sqlCompile_storage as storage

    path = tmp_path / "manual.csv"
    write_manual_status_rows(manual_row(), path)
    original = path.read_bytes()

    def fail(*args):
        raise PermissionError("File is open")

    monkeypatch.setattr(storage.os, "replace", fail)
    with pytest.raises(PermissionError):
        write_manual_status_rows(manual_row(status="RS"), path)
    assert path.read_bytes() == original
    backups = list((tmp_path / "_backups" / path.name).glob("*.csv"))
    assert len(backups) == 1 and backups[0].read_bytes() == original
    assert not list(tmp_path.glob("*.tmp"))


def test_database_publish_is_atomic_and_preserves_other_tables(tmp_path, monkeypatch):
    path = tmp_path / "data.sqlite"
    original = pd.DataFrame([["Fall 2020", "Alpha", "0001", "N"]], columns=OUTPUT_COLUMNS)
    write_sqlite(original, path)
    with atomic_database_update(path) as connection:
        connection.execute("CREATE TABLE my_notes (note TEXT)")
        connection.execute("INSERT INTO my_notes VALUES ('keep')")

    def fail(*args, **kwargs):
        raise RuntimeError("Side table failed")

    with monkeypatch.context() as scoped:
        scoped.setattr(pd.DataFrame, "to_sql", fail)
        with pytest.raises(RuntimeError, match="Side table"):
            write_sqlite(original.assign(Status="G"), path)
    with read_database(path) as connection:
        assert connection.execute('SELECT Status FROM sqlCompile').fetchone()[0] == "N"
    write_sqlite(original.assign(Status="G"), path)
    with read_database(path) as connection:
        assert connection.execute('SELECT Status FROM sqlCompile').fetchone()[0] == "G"
        assert connection.execute('SELECT note FROM my_notes').fetchone()[0] == "keep"
    assert list((tmp_path / "_backups" / path.name).glob("*.sqlite"))
    assert not list(tmp_path.glob("*.tmp"))
