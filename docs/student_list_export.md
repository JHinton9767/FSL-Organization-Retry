# Export the complete FSL student list

Run this on the computer that has the populated SQL database. Transfer the
updated code to that computer first. Double-click `Export_FSL_Student_List.bat`
in the project folder, or run from PowerShell:

```powershell
uv run --no-project python export_fsl_student_list.py
```

If Python is installed without uv, use `py export_fsl_student_list.py`.
The exporter uses only Python's standard library.

## Files

The exporter writes to `data/outgoing/fsl_student_list/`:

- **`fsl_student_list.csv`** is the file to send. It contains only `Student ID`
  and `Student Name`, sorted by ID, with one row per unique nonblank ID.
- **`fsl_student_name_review.csv`** lists missing names, unresolved conflicting
  names, and unexpected ID formats. Check these rows before sharing. It contains
  the displayed name and all observed names for each flagged ID. When there are
  no issues, it contains only its header.

Both files open in Excel and use UTF-8 with a BOM to preserve accented names.
Repeated runs replace these two generated files. The source database and saved
name corrections are read only. An empty or missing database fails without
replacing an earlier export.

## Who is included and how names are selected

The population is the union of IDs in `sqlCompile`,
`sqlCompile_student_names`, `sqlCompile_student_name_observations`, and
`sqlCompile_new_member_observations` when those side tables exist. There is no
semester, chapter, status, cohort, council, or dashboard cutoff filter. Students
with missing names are retained. The same name under two different IDs stays
as two students. IDs are trimmed, internal whitespace is collapsed, and letters
are uppercased. Unexpected formats are retained and flagged rather than dropped.

The exporter does not use previously generated cohort report tables, which can
contain older results from before the most recent roster compile. It does not
add IDs found only in a manual name-correction file. It can only include IDs
present in the compiled database; it does not scan the raw source files.

Names are selected in this order:

1. The latest nonblank saved correction in
   `config/sqlCompile_duplicate_name_resolutions.csv`, when present.
2. The existing compiled student-name lookup.
3. The most frequently observed available name, with alphabetical tie-breaking.

Name differences beyond case and whitespace appear in the review file unless
a saved name correction already resolves them. A provisional name is still
shown in the main list so every ID has one row. This export does not resolve
identity conflicts or assign final outcome buckets.

Older databases may have no name tables. The IDs still export, missing names
are flagged, and a warning explains that names or historical name variations
are unavailable. To update those tables, run `uv run --with-requirements
requirements.txt python sqlCompile.py` on the data computer with the full
configured roster source folders available, then rerun the export.

## Custom paths

```powershell
py export_fsl_student_list.py --database "S:\FSL\sqlCompile.sqlite" --output-dir "C:\FSL\student_request"
```

Use `--name-choices "path\to\sqlCompile_duplicate_name_resolutions.csv"` if
the saved name corrections are elsewhere. Relative paths resolve from the
project folder. `--table` supports a custom base roster table name.

## Suggested request to accompany the list

Please use the attached student IDs to return each student's full available
term-by-term enrollment history, including terms before joining FSL, during FSL
participation, and after their last FSL appearance. Please also return verified
institutional outcome categories, the effective term/date for each outcome,
degree award dates where applicable, and the as-of date of the extract. Flag
unmatched IDs and distinguish unknown outcomes from confirmed ones.

These IDs define the population to look up. The list itself does not establish
FSL entry/exit dates or final institutional outcomes. Later, the returned
history can be joined by Student ID to the existing FSL semester records.
