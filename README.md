# Greek Life Academic Analytics Pipeline

This project provides a scalable MVP pipeline for fraternity/sorority academic analytics from 2012-present. It is designed around:

- Folder-based ingestion for academic and roster files
- Automatic column standardization across inconsistent schemas
- Identity resolution with Student ID first and name/email fallback
- A single long-format master dataset
- Cohort-based metrics for graduation, retention, GPA, credit momentum, and academic standing
- Excel/Power Query-friendly outputs

## Project layout

```text
config/
  column_aliases.json
data/
  inbox/
    academic/
    rosters/
  raw/
    academic/
    rosters/
  processed/
output/
  excel/
  metrics/
powerquery/
  AcademicFolderTransform.pq
  RosterFolderTransform.pq
  MasterDataset.pq
src/
  build_yearly_chapter_rosters.py
  greek_life_pipeline.py
run_yearly_chapter_rosters.py
run_pipeline.py
requirements.txt
```

## What the pipeline does

1. Reads all `.csv`, `.xlsx`, and `.xls` files from `data/inbox/academic`, `data/inbox/rosters`, `data/raw/academic`, and `data/raw/rosters`
2. Maps inconsistent source columns to a standard schema
3. Normalizes terms into sortable term keys
4. Resolves identities using:
   - `StudentID`
   - email
   - `FirstName + LastName + Email`
   - `FirstName + LastName`
5. Builds a master long-format dataset with Greek membership enrichment
6. Assigns student cohorts using first enrollment term and status text
7. Produces metrics tables and an Excel workbook with year-separated sheets in ~1000-row blocks

## Standard output tables

- `data/processed/master_dataset.csv`
- `data/processed/student_summary.csv`
- `output/metrics/graduation_rates.csv`
- `output/metrics/retention_rates.csv`
- `output/metrics/gpa_trends.csv`
- `output/metrics/credit_momentum.csv`
- `output/metrics/standing_distribution.csv`
- `output/excel/greek_life_master.xlsx`

## Run

For the easiest manual workflow, drag and drop source files into:

- `data/inbox/academic`
- `data/inbox/rosters`

The pipeline also still supports the original folders:

- `data/raw/academic`
- `data/raw/rosters`

Then run:

```powershell
python -m pip install -r requirements.txt
python run_pipeline.py
```

Files in `data/inbox/` and generated outputs are ignored by Git so you can work locally without committing source data.

## Master roster helpers

Once `Master_FSL_Roster.xlsx` has been created, you can generate yearly chapter workbooks:

```powershell
python run_yearly_chapter_rosters.py
```

This writes a `Yearly/` folder where:

- each workbook is one academic year like `2015.xlsx`
- each sheet is one chapter present that year
- each sheet contains `Last Name`, `First Name`, and `Banner ID`

## Excel / Power Query workflow

The `powerquery/` folder contains M queries you can paste into Excel Power Query:

- `AcademicFolderTransform.pq`: folder-based academic file ingestion
- `RosterFolderTransform.pq`: folder-based roster ingestion
- `MasterDataset.pq`: joins standardized academic and roster queries into a master model

Recommended Excel setup:

1. Create one workbook for the master model.
2. Add Power Query connections for academic and roster folders.
3. Load the final master query to a table.
4. Build pivots from the master table and/or from the Python-produced metrics tables.
5. Refresh from folder whenever new files arrive.

## Structural notes

- The Python exporter writes year-based Excel sheets with 1000-row blocks to align with the requested workbook structure.
- The master dataset is long-format and extension-ready for Pell, major, and Greek vs non-Greek controls when those columns become available.
- The pipeline is non-interactive by design: configuration lives in `config/column_aliases.json`.

## Assumptions in the MVP

- `Credits_Earned` may not exist in some academic files; when absent, the pipeline preserves nulls unless it can infer a value safely.
- Graduation can come from explicit graduate/alumni status or a source grad indicator; otherwise the student remains non-graduated in the MVP.
- Cohorts are assigned from the earliest observed enrollment term and status text.
