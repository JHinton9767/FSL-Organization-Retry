import csv
import hashlib
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from contextlib import closing
from pathlib import Path

from src.sqlCompile_student_export import export_student_list, main


class StudentListExportTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.database = self.root / "student data.sqlite"
        self.output = self.root / "outgoing"
        self.choices = self.root / "names.csv"
        self.write_choices([])
        with closing(sqlite3.connect(self.database)) as connection, connection:
            connection.execute('CREATE TABLE sqlCompile (Semester TEXT, Chapter TEXT, "Student ID" TEXT, Status TEXT)')

    def write_choices(self, rows):
        with self.choices.open("w", encoding="utf-8-sig", newline="") as handle:
            writer = csv.writer(handle)
            writer.writerow(["Student ID", "Student Name"])
            writer.writerows(rows)

    def add_roster(self, rows):
        with closing(sqlite3.connect(self.database)) as connection, connection:
            connection.executemany("INSERT INTO sqlCompile VALUES (?, ?, ?, ?)", rows)

    def add_names(self, rows, *, observations=False):
        table = "sqlCompile_student_name_observations" if observations else "sqlCompile_student_names"
        count_column = ', "Observation Count" INTEGER' if observations else ""
        placeholders = "?, ?, ?" if observations else "?, ?"
        with closing(sqlite3.connect(self.database)) as connection, connection:
            connection.execute(f'CREATE TABLE {table} ("Student ID" TEXT, "Student Name" TEXT{count_column})')
            connection.executemany(f"INSERT INTO {table} VALUES ({placeholders})", rows)

    def export(self, **kwargs):
        return export_student_list(self.database, self.output, name_choices_path=self.choices, **kwargs)

    def read_csv(self, path):
        with path.open(encoding="utf-8-sig", newline="") as handle:
            return list(csv.DictReader(handle))

    def test_all_statuses_and_terms_deduplicate_by_id_not_name(self):
        self.add_roster([
            ("Fall 2010", "Alpha", " a01234567 ", "N"),
            ("Spring 2011", "Beta", "A01234567", "G"),
            ("Fall 2026", "Alpha", "A01234568", "A"),
            ("Fall 2024", "Gamma", "A01234569", "RS"),
        ])
        self.add_names([
            ("A01234567", "Rivera, Ana"),
            ("A01234568", "Rivera, Ana"),
            ("A01234569", "Chen, Morgan"),
        ])
        result = self.export()
        rows = self.read_csv(result.list_path)
        self.assertEqual(result.student_count, 3)
        self.assertEqual(list(rows[0]), ["Student ID", "Student Name"])
        self.assertEqual([row["Student ID"] for row in rows], ["A01234567", "A01234568", "A01234569"])
        self.assertEqual(result.missing_name_count, 0)

    def test_union_includes_side_table_ids_but_not_stale_cohorts_or_manual_only_ids(self):
        self.add_roster([("Fall 2025", "Alpha", "A01234567", "A")])
        self.add_names([("A01234568", "Name, Two")])
        self.add_names([("A01234569", "Name, Three", 3)], observations=True)
        self.write_choices([("A09999999", "Unrelated, Student")])
        with closing(sqlite3.connect(self.database)) as connection, connection:
            connection.execute('CREATE TABLE sqlCompile_new_member_observations ("Student ID" TEXT)')
            connection.execute("INSERT INTO sqlCompile_new_member_observations VALUES ('A01234570')")
            connection.execute('CREATE TABLE new_member_outcomes ("Student ID" TEXT)')
            connection.execute("INSERT INTO new_member_outcomes VALUES ('A08888888')")
        result = self.export()
        self.assertEqual([row["Student ID"] for row in self.read_csv(result.list_path)], [
            "A01234567", "A01234568", "A01234569", "A01234570",
        ])

    def test_compiled_name_wins_and_unresolved_alternatives_are_reviewed(self):
        self.add_names([("A01234567", "Preferred, Ana")])
        self.add_names([
            ("A01234567", "Other, Anne", 10),
            ("A01234567", "Preferred, Ana", 2),
        ], observations=True)
        result = self.export()
        self.assertEqual(self.read_csv(result.list_path)[0]["Student Name"], "Preferred, Ana")
        review = self.read_csv(result.review_path)[0]
        self.assertIn("provisional", review["Review Reason"])
        self.assertIn("Other, Anne", review["Observed Names"])
        self.assertEqual(result.conflicting_name_count, 1)

    def test_latest_nonblank_saved_name_resolves_conflict(self):
        self.add_names([("A01234567", "Original, Ana")])
        self.add_names([("A01234567", "Variant, Ana", 20)], observations=True)
        self.write_choices([
            ("A01234567", "Earlier, Ana"),
            (" a01234567 ", "Corrected, Ana"),
            ("A01234567", ""),
        ])
        result = self.export()
        self.assertEqual(self.read_csv(result.list_path)[0]["Student Name"], "Corrected, Ana")
        self.assertEqual(result.review_count, 0)

    def test_observation_fallback_uses_frequency_and_deterministic_tie(self):
        self.add_names([
            ("A01234567", "Zulu, Ana", 4),
            ("A01234567", "Alpha, Ana", 4),
            ("A01234568", "Common, Sam", 9),
            ("A01234568", "Rare, Sam", 1),
        ], observations=True)
        result = self.export()
        self.assertEqual([row["Student Name"] for row in self.read_csv(result.list_path)], ["Alpha, Ana", "Common, Sam"])

    def test_case_and_spacing_variations_do_not_require_review(self):
        self.add_names([("A01234567", "Rivera, Ana", 2), ("A01234567", "RIVERA,  ANA", 1)], observations=True)
        result = self.export()
        self.assertEqual(result.review_count, 0)

    def test_legacy_database_retains_missing_names_and_unexpected_ids(self):
        self.add_roster([
            ("Fall 2025", "Alpha", "A01234567", "A"),
            ("Fall 2025", "Alpha", "unexpected-id", "A"),
            ("Fall 2025", "Alpha", None, "A"),
            ("Fall 2025", "Alpha", " ", "A"),
        ])
        result = self.export()
        self.assertEqual(result.student_count, 2)
        self.assertEqual(result.missing_name_count, 2)
        self.assertEqual(result.review_count, 2)
        self.assertIn("Unexpected student ID format", self.read_csv(result.review_path)[1]["Review Reason"])
        self.assertTrue(any("Skipped 2" in warning for warning in result.warnings))
        self.assertTrue(any("lookup is missing" in warning for warning in result.warnings))

    def test_empty_database_does_not_replace_existing_export(self):
        self.output.mkdir()
        previous = self.output / "fsl_student_list.csv"
        previous.write_text("Previous export", encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "no student IDs"):
            self.export()
        self.assertEqual(previous.read_text(encoding="utf-8"), "Previous export")
        self.assertFalse((self.output / "fsl_student_name_review.csv").exists())

    def test_missing_database_is_not_created(self):
        self.database.unlink()
        with self.assertRaises(FileNotFoundError):
            self.export()
        self.assertFalse(self.database.exists())
        self.assertFalse(self.output.exists())

    def test_bad_schema_or_explicit_missing_choices_fail_without_output(self):
        self.add_roster([("Fall 2025", "Alpha", "A01234567", "A")])
        self.choices.unlink()
        with self.assertRaises(FileNotFoundError):
            self.export()
        self.assertFalse(self.output.exists())
        self.write_choices([])
        with closing(sqlite3.connect(self.database)) as connection, connection:
            connection.execute("CREATE TABLE sqlCompile_student_names (wrong_column TEXT)")
        with self.assertRaisesRegex(ValueError, "missing required columns"):
            self.export()
        self.assertFalse(self.output.exists())

    def test_unicode_csv_round_trip_read_only_sources_and_cleared_review(self):
        self.add_names([("A01234567", 'García, Ana "Annie"')])
        before_database = hashlib.sha256(self.database.read_bytes()).hexdigest()
        before_choices = self.choices.read_bytes()
        result = self.export()
        self.assertEqual(self.read_csv(result.list_path)[0]["Student Name"], 'García, Ana "Annie"')
        self.assertTrue(result.list_path.read_bytes().startswith(b"\xef\xbb\xbf"))
        self.assertEqual(hashlib.sha256(self.database.read_bytes()).hexdigest(), before_database)
        self.assertEqual(self.choices.read_bytes(), before_choices)
        result.review_path.write_text("Stale review", encoding="utf-8")
        self.export()
        self.assertEqual(self.read_csv(result.review_path), [])

    def test_custom_table_identifier_and_missing_table(self):
        table = 'Roster " history'
        with closing(sqlite3.connect(self.database)) as connection, connection:
            connection.execute('CREATE TABLE "Roster "" history" ("Student ID" TEXT, "Student Name" TEXT)')
            connection.execute('INSERT INTO "Roster "" history" VALUES (?, ?)', ("A01234567", "Rivera, Ana"))
        result = self.export(table_name=table)
        self.assertEqual(result.student_count, 1)
        with self.assertRaisesRegex(ValueError, "missing"):
            self.export(table_name="does not exist")

    def test_cli_from_another_directory_and_failure_exit_code(self):
        self.add_names([("A01234567", "Rivera, Ana")])
        script = Path(__file__).resolve().parents[1] / "export_fsl_student_list.py"
        run = subprocess.run([
            sys.executable, str(script), "--database", str(self.database),
            "--output-dir", str(self.output), "--name-choices", str(self.choices),
        ], cwd=self.root, text=True, capture_output=True)
        self.assertEqual(run.returncode, 0, run.stderr)
        self.assertIn("Unique students: 1", run.stdout)
        self.assertTrue((self.output / "fsl_student_list.csv").is_file())
        self.assertEqual(main(["--database", str(self.root / "absent.sqlite")]), 1)


if __name__ == "__main__":
    unittest.main()
