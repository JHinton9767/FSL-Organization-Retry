from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import pandas as pd


ROOT = Path(__file__).resolve().parent.parent
CONFIG_PATH = ROOT / "config" / "column_aliases.json"
INBOX_ACADEMIC_DIR = ROOT / "data" / "inbox" / "academic"
INBOX_ROSTER_DIR = ROOT / "data" / "inbox" / "rosters"
RAW_ACADEMIC_DIR = ROOT / "data" / "raw" / "academic"
RAW_ROSTER_DIR = ROOT / "data" / "raw" / "rosters"
PROCESSED_DIR = ROOT / "data" / "processed"
METRICS_DIR = ROOT / "output" / "metrics"
EXCEL_DIR = ROOT / "output" / "excel"


TERM_ORDER = {
    "winter": 0,
    "spring": 1,
    "summer": 2,
    "fall": 3,
    "unknown": 9,
}

ACTIVE_MEMBERSHIP_CODES = {"A", "N", "T", "MEMBER", "COUNCIL"}
GRADUATED_MEMBERSHIP_CODES = {"G", "AL", "GRADUATED", "ALUMNI"}


@dataclass
class TermParts:
    year: Optional[int]
    season: str

    @property
    def sort_key(self) -> Optional[int]:
        if self.year is None:
            return None
        return self.year * 10 + TERM_ORDER.get(self.season, TERM_ORDER["unknown"])

    @property
    def label(self) -> Optional[str]:
        if self.year is None:
            return None
        return f"{self.year} {self.season.title()}"


def load_aliases() -> Dict[str, List[str]]:
    with CONFIG_PATH.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def canonicalize_column(name: object) -> str:
    text = "" if name is None else str(name)
    text = text.strip().lower()
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def build_alias_lookup() -> Dict[str, str]:
    aliases = load_aliases()
    lookup: Dict[str, str] = {}
    for standard_name, variants in aliases.items():
        lookup[canonicalize_column(standard_name)] = standard_name
        for variant in variants:
            lookup[canonicalize_column(variant)] = standard_name
    return lookup


def ensure_directories() -> None:
    for path in [
        INBOX_ACADEMIC_DIR,
        INBOX_ROSTER_DIR,
        RAW_ACADEMIC_DIR,
        RAW_ROSTER_DIR,
        PROCESSED_DIR,
        METRICS_DIR,
        EXCEL_DIR,
    ]:
        path.mkdir(parents=True, exist_ok=True)


def list_source_files(folder: Path) -> List[Path]:
    supported = {".csv", ".xlsx", ".xls"}
    return sorted(path for path in folder.rglob("*") if path.suffix.lower() in supported)


def list_source_files_from_folders(folders: Iterable[Path]) -> List[Path]:
    files: List[Path] = []
    seen: set[Path] = set()
    for folder in folders:
        for path in list_source_files(folder):
            resolved = path.resolve()
            if resolved not in seen:
                seen.add(resolved)
                files.append(path)
    return sorted(files)


def read_file(path: Path) -> pd.DataFrame:
    if path.suffix.lower() == ".csv":
        return pd.read_csv(path)
    return pd.read_excel(path)


def standardize_columns(df: pd.DataFrame, alias_lookup: Dict[str, str]) -> pd.DataFrame:
    renamed = {}
    used_targets = set()
    for column in df.columns:
        key = canonicalize_column(column)
        target = alias_lookup.get(key)
        if target and target not in used_targets:
            renamed[column] = target
            used_targets.add(target)
        else:
            renamed[column] = re.sub(r"\s+", "_", key) if key else "unnamed_column"
    return df.rename(columns=renamed)


def combine_sources(folders: Iterable[Path], source_type: str, alias_lookup: Dict[str, str]) -> pd.DataFrame:
    frames = []
    for path in list_source_files_from_folders(folders):
        frame = read_file(path)
        frame = standardize_columns(frame, alias_lookup)
        frame["source_file"] = path.name
        frame["source_type"] = source_type
        frames.append(frame)
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True, sort=False)


def clean_text_series(series: pd.Series) -> pd.Series:
    return (
        series.fillna("")
        .astype(str)
        .str.strip()
        .replace({"nan": "", "None": "", "NONE": ""})
    )


def normalize_student_id(value: object) -> Optional[str]:
    if pd.isna(value):
        return None
    text = str(value).strip()
    if not text or text.lower() == "nan":
        return None
    return re.sub(r"\.0$", "", text)


def parse_term(value: object) -> TermParts:
    if pd.isna(value):
        return TermParts(year=None, season="unknown")
    text = str(value).strip().lower()
    if not text:
        return TermParts(year=None, season="unknown")

    year_match = re.search(r"(20\d{2}|19\d{2})", text)
    year = int(year_match.group(1)) if year_match else None

    if re.search(r"(?:\b|_)(fa|fall)(?:\b|_)", text) or "fall" in text or text.endswith("fa") or text.startswith("fa"):
        season = "fall"
    elif re.search(r"(?:\b|_)(sp|spr|spring)(?:\b|_)", text) or "spring" in text or text.endswith("sp") or text.startswith("sp"):
        season = "spring"
    elif re.search(r"(?:\b|_)(sum|su|summer)(?:\b|_)", text) or "summer" in text or text.endswith("su") or text.startswith("su"):
        season = "summer"
    elif re.search(r"(?:\b|_)(win|winter)(?:\b|_)", text) or "winter" in text or text.endswith("wi") or text.startswith("wi"):
        season = "winter"
    elif re.fullmatch(r"\d{6}", text):
        code = text[-2:]
        season = {"10": "spring", "20": "summer", "30": "fall"}.get(code, "unknown")
        if year is None:
            year = int(text[:4])
    else:
        season = "unknown"

    return TermParts(year=year, season=season)


def add_term_columns(df: pd.DataFrame, source_term_column: str) -> pd.DataFrame:
    if source_term_column not in df.columns:
        df[source_term_column] = pd.NA
    parsed = df[source_term_column].apply(parse_term)
    df["term"] = parsed.apply(lambda item: item.label)
    df["year"] = parsed.apply(lambda item: item.year)
    df["term_sort"] = parsed.apply(lambda item: item.sort_key)
    return df


def normalize_academic_records(df: pd.DataFrame) -> pd.DataFrame:
    expected = [
        "student_id",
        "first_name",
        "last_name",
        "email",
        "student_status",
        "major",
        "term",
        "year",
        "term_sort",
        "gpa_term",
        "gpa_cum",
        "transfer_gpa",
        "credits_attempted",
        "credits_earned",
        "academic_standing",
        "pell_flag",
        "graduation_term",
        "graduation_year",
        "source_file",
    ]
    if df.empty:
        return pd.DataFrame(columns=expected)

    for column in ["first_name", "last_name", "email", "student_status", "major", "academic_standing"]:
        if column in df.columns:
            df[column] = clean_text_series(df[column])

    df = add_term_columns(df, "term")
    df["student_id"] = df["student_id"].apply(normalize_student_id) if "student_id" in df.columns else None

    for column in ["gpa_term", "gpa_cum", "transfer_gpa", "credits_attempted", "credits_earned", "graduation_year"]:
        if column in df.columns:
            df[column] = pd.to_numeric(df[column], errors="coerce")
        else:
            df[column] = pd.NA

    for column in expected:
        if column not in df.columns:
            df[column] = pd.NA

    dedupe_key = ["student_id", "email", "first_name", "last_name", "term", "source_file"]
    return df[expected].drop_duplicates(subset=dedupe_key)


def normalize_roster_records(df: pd.DataFrame) -> pd.DataFrame:
    expected = [
        "student_id",
        "first_name",
        "last_name",
        "email",
        "chapter",
        "membership_status",
        "semester_joined",
        "join_term",
        "join_year",
        "join_term_sort",
        "position",
        "source_file",
    ]
    if df.empty:
        return pd.DataFrame(columns=expected)

    for column in ["first_name", "last_name", "email", "chapter", "membership_status", "position"]:
        if column in df.columns:
            df[column] = clean_text_series(df[column])

    df["student_id"] = df["student_id"].apply(normalize_student_id) if "student_id" in df.columns else None
    if "semester_joined" not in df.columns:
        df["semester_joined"] = pd.NA
    df = add_term_columns(df, "semester_joined")
    df["join_term"] = df["term"]
    df["join_year"] = df["year"]
    df["join_term_sort"] = df["term_sort"]
    df["membership_status"] = df["membership_status"].str.upper()

    for column in expected:
        if column not in df.columns:
            df[column] = pd.NA

    dedupe_key = ["student_id", "email", "first_name", "last_name", "chapter", "membership_status", "join_term"]
    return df[expected].drop_duplicates(subset=dedupe_key)


def build_identity_lookup(academic_df: pd.DataFrame) -> Dict[Tuple[str, str], str]:
    lookup: Dict[Tuple[str, str], str] = {}
    source = academic_df.copy()
    source["email_key"] = clean_text_series(source["email"]).str.lower()
    source["name_key"] = clean_text_series(source["first_name"]).str.lower() + "|" + clean_text_series(source["last_name"]).str.lower()
    source = source[source["student_id"].notna()]

    for _, row in source.iterrows():
        if row["email_key"]:
            lookup[("email", row["email_key"])] = row["student_id"]
        if row["name_key"] != "|":
            lookup[("name", row["name_key"])] = row["student_id"]
        if row["email_key"] and row["name_key"] != "|":
            lookup[("email_name", row["email_key"] + "|" + row["name_key"])] = row["student_id"]
    return lookup


def resolve_missing_roster_ids(roster_df: pd.DataFrame, academic_df: pd.DataFrame) -> pd.DataFrame:
    if roster_df.empty:
        return roster_df

    lookup = build_identity_lookup(academic_df)
    resolved = roster_df.copy()
    resolved["email_key"] = clean_text_series(resolved["email"]).str.lower()
    resolved["name_key"] = clean_text_series(resolved["first_name"]).str.lower() + "|" + clean_text_series(resolved["last_name"]).str.lower()

    inferred_ids = []
    for idx, row in resolved.iterrows():
        student_id = row["student_id"]
        if student_id:
            inferred_ids.append(student_id)
            continue

        candidate = None
        if row["email_key"] and row["name_key"] != "|":
            candidate = lookup.get(("email_name", row["email_key"] + "|" + row["name_key"]))
        if candidate is None and row["email_key"]:
            candidate = lookup.get(("email", row["email_key"]))
        if candidate is None and row["name_key"] != "|":
            candidate = lookup.get(("name", row["name_key"]))
        if candidate is None:
            candidate = f"INF-{idx + 1:07d}"
        inferred_ids.append(candidate)

    resolved["student_id"] = inferred_ids
    return resolved.drop(columns=["email_key", "name_key"])


def infer_cohort(student_status: object) -> str:
    text = "" if pd.isna(student_status) else str(student_status).strip().lower()
    if "transfer" in text:
        return "Transfer"
    if "readmit" in text or "re-admit" in text or "return" in text:
        return "Readmit"
    return "FTFT"


def attach_membership(academic_df: pd.DataFrame, roster_df: pd.DataFrame) -> pd.DataFrame:
    if academic_df.empty:
        return academic_df

    roster = roster_df.sort_values(["student_id", "join_term_sort", "chapter"], na_position="last").copy()
    grouped = roster.groupby("student_id", dropna=False)

    enriched_rows = []
    academic_sorted = academic_df.sort_values(["student_id", "term_sort"])
    for student_id, student_terms in academic_sorted.groupby("student_id", dropna=False):
        roster_rows = grouped.get_group(student_id) if student_id in grouped.groups else pd.DataFrame(columns=roster.columns)
        earliest_join = roster_rows.sort_values("join_term_sort", na_position="last").iloc[0] if not roster_rows.empty else None
        for _, academic_row in student_terms.iterrows():
            eligible = roster_rows
            if pd.notna(academic_row["term_sort"]):
                eligible = roster_rows[roster_rows["join_term_sort"].fillna(999999) <= academic_row["term_sort"]]
            latest = eligible.iloc[-1] if not eligible.empty else None

            chapter = latest["chapter"] if latest is not None else pd.NA
            membership_status = latest["membership_status"] if latest is not None else pd.NA
            join_term = earliest_join["join_term"] if earliest_join is not None else pd.NA
            join_term_sort = earliest_join["join_term_sort"] if earliest_join is not None else pd.NA
            active_by_term = bool(latest is not None and str(membership_status).upper() in ACTIVE_MEMBERSHIP_CODES)

            time_in_greek = pd.NA
            if latest is not None and pd.notna(join_term_sort) and pd.notna(academic_row["term_sort"]):
                time_in_greek = max(int(academic_row["term_sort"] - join_term_sort), 0)

            item = academic_row.to_dict()
            item.update(
                {
                    "chapter": chapter,
                    "greek_status": membership_status,
                    "join_term": join_term,
                    "join_term_sort": join_term_sort,
                    "active_by_term": active_by_term,
                    "time_in_greek": time_in_greek,
                }
            )
            enriched_rows.append(item)

    return pd.DataFrame(enriched_rows)


def summarize_students(master_df: pd.DataFrame, roster_df: pd.DataFrame) -> pd.DataFrame:
    membership_statuses = (
        roster_df.sort_values(["student_id", "join_term_sort"])
        .groupby("student_id", dropna=False)
        .agg(
            chapter=("chapter", "last"),
            latest_membership_status=("membership_status", "last"),
            join_term=("join_term", "first"),
            join_term_sort=("join_term_sort", "first"),
        )
        .reset_index()
    )

    summary = (
        master_df.sort_values(["student_id", "term_sort"])
        .groupby("student_id", dropna=False)
        .agg(
            first_term=("term", "first"),
            first_year=("year", "first"),
            first_term_sort=("term_sort", "first"),
            last_term=("term", "last"),
            last_year=("year", "last"),
            last_term_sort=("term_sort", "last"),
            cohort=("cohort", "first"),
            major=("major", "first"),
            pell_flag=("pell_flag", "first"),
            latest_gpa_cum=("gpa_cum", "last"),
            avg_term_gpa=("gpa_term", "mean"),
            total_attempted=("credits_attempted", "sum"),
            total_earned=("credits_earned", "sum"),
            first_name=("first_name", "first"),
            last_name=("last_name", "first"),
            email=("email", "first"),
        )
        .reset_index()
    )

    summary = summary.merge(membership_statuses, on="student_id", how="left")
    summary["graduated"] = summary["latest_membership_status"].astype(str).str.upper().isin(GRADUATED_MEMBERSHIP_CODES)
    summary["years_to_last_seen"] = (summary["last_term_sort"] - summary["first_term_sort"]) / 10.0
    summary["graduated_4yr"] = summary["graduated"] & (summary["years_to_last_seen"] <= 4.0)
    summary["graduated_6yr"] = summary["graduated"] & (summary["years_to_last_seen"] <= 6.0)
    return summary


def build_master_dataset(academic_df: pd.DataFrame, roster_df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    roster_df = resolve_missing_roster_ids(roster_df, academic_df)

    first_enrollment = (
        academic_df.sort_values(["student_id", "term_sort"])
        .groupby("student_id", dropna=False)
        .agg(
            first_term=("term", "first"),
            first_year=("year", "first"),
            first_term_sort=("term_sort", "first"),
            cohort=("student_status", lambda series: infer_cohort(series.dropna().iloc[0] if not series.dropna().empty else "")),
            major=("major", "first"),
            pell_flag=("pell_flag", "first"),
        )
        .reset_index()
    )

    master_df = attach_membership(academic_df, roster_df)
    master_df = master_df.merge(first_enrollment, on="student_id", how="left", suffixes=("", "_student"))
    master_df["cohort_year"] = master_df["first_year"]
    master_df["greek_indicator"] = master_df["chapter"].notna()

    columns = [
        "student_id",
        "term",
        "year",
        "term_sort",
        "chapter",
        "cohort",
        "cohort_year",
        "gpa_term",
        "gpa_cum",
        "credits_attempted",
        "credits_earned",
        "academic_standing",
        "greek_status",
        "join_term",
        "active_by_term",
        "time_in_greek",
        "student_status",
        "major",
        "pell_flag",
        "first_name",
        "last_name",
        "email",
        "greek_indicator",
    ]
    for column in columns:
        if column not in master_df.columns:
            master_df[column] = pd.NA

    student_summary = summarize_students(master_df, roster_df)
    return master_df[columns].sort_values(["student_id", "term_sort"]), student_summary


def calculate_graduation_rates(student_summary: pd.DataFrame) -> pd.DataFrame:
    grouped = (
        student_summary.groupby(["cohort", "first_year", "chapter"], dropna=False)
        .agg(
            students=("student_id", "nunique"),
            graduates_4yr=("graduated_4yr", "sum"),
            graduates_6yr=("graduated_6yr", "sum"),
        )
        .reset_index()
        .rename(columns={"first_year": "cohort_year"})
    )
    grouped["grad_rate_4yr"] = grouped["graduates_4yr"] / grouped["students"]
    grouped["grad_rate_6yr"] = grouped["graduates_6yr"] / grouped["students"]
    return grouped


def calculate_retention_rates(student_summary: pd.DataFrame) -> pd.DataFrame:
    retained = student_summary.copy()
    retained["retained_to_year_2"] = (retained["last_term_sort"] - retained["first_term_sort"]) >= 10
    result = (
        retained.groupby(["cohort", "first_year", "chapter"], dropna=False)
        .agg(
            students=("student_id", "nunique"),
            retained_to_year_2=("retained_to_year_2", "sum"),
        )
        .reset_index()
        .rename(columns={"first_year": "cohort_year"})
    )
    result["retention_rate_year_2"] = result["retained_to_year_2"] / result["students"]
    return result


def calculate_gpa_trends(master_df: pd.DataFrame) -> pd.DataFrame:
    frame = master_df.sort_values(["student_id", "term_sort"]).copy()
    frame["prior_term_gpa"] = frame.groupby("student_id", dropna=False)["gpa_term"].shift(1)
    frame["term_gpa_delta"] = frame["gpa_term"] - frame["prior_term_gpa"]

    def membership_phase(row: pd.Series) -> str:
        if pd.isna(row["join_term"]):
            return "No Greek Affiliation"
        if pd.notna(row["join_term_sort"]) and pd.notna(row["term_sort"]) and row["term_sort"] < row["join_term_sort"]:
            return "Pre Join"
        if row["active_by_term"]:
            return "Post Join"
        return "Post Join Inactive"

    frame["membership_phase"] = frame.apply(membership_phase, axis=1)
    return (
        frame.groupby(["year", "chapter", "cohort", "membership_phase"], dropna=False)
        .agg(
            students=("student_id", "nunique"),
            avg_term_gpa=("gpa_term", "mean"),
            avg_cum_gpa=("gpa_cum", "mean"),
            avg_term_gpa_delta=("term_gpa_delta", "mean"),
        )
        .reset_index()
    )


def calculate_credit_momentum(master_df: pd.DataFrame) -> pd.DataFrame:
    frame = master_df.copy()
    frame["credit_momentum"] = frame["credits_earned"] / frame["credits_attempted"]
    frame["full_time_term"] = frame["credits_attempted"] >= 12
    frame["part_time_term"] = frame["credits_attempted"].between(0, 11.999, inclusive="both")
    frame["withdrawal_or_incomplete"] = frame["credits_earned"] < frame["credits_attempted"]
    return (
        frame.groupby(["year", "chapter", "cohort"], dropna=False)
        .agg(
            students=("student_id", "nunique"),
            avg_credits_attempted=("credits_attempted", "mean"),
            avg_credits_earned=("credits_earned", "mean"),
            avg_credit_momentum=("credit_momentum", "mean"),
            full_time_terms=("full_time_term", "sum"),
            part_time_terms=("part_time_term", "sum"),
            withdrawal_or_incomplete_terms=("withdrawal_or_incomplete", "sum"),
        )
        .reset_index()
    )


def calculate_standing_distribution(master_df: pd.DataFrame) -> pd.DataFrame:
    frame = master_df.copy()
    frame["standing_group"] = frame["academic_standing"].fillna("Unknown").replace("", "Unknown")
    counts = (
        frame.groupby(["year", "chapter", "cohort", "standing_group"], dropna=False)
        .agg(records=("student_id", "count"), students=("student_id", "nunique"))
        .reset_index()
    )
    totals = counts.groupby(["year", "chapter", "cohort"], dropna=False)["records"].transform("sum")
    counts["record_share"] = counts["records"] / totals
    return counts


def export_csv(df: pd.DataFrame, path: Path) -> None:
    df.to_csv(path, index=False)


def chunk_dataframe(df: pd.DataFrame, chunk_size: int) -> Iterable[pd.DataFrame]:
    if df.empty:
        yield df
        return
    for start in range(0, len(df), chunk_size):
        yield df.iloc[start : start + chunk_size]


def export_excel(master_df: pd.DataFrame, metrics: Dict[str, pd.DataFrame], path: Path) -> None:
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        if master_df.empty:
            pd.DataFrame(columns=master_df.columns).to_excel(writer, sheet_name="Master_Empty", index=False)
        else:
            years = [year for year in sorted(master_df["year"].dropna().unique())]
            for year in years:
                year_df = master_df[master_df["year"] == year].reset_index(drop=True)
                for idx, block in enumerate(chunk_dataframe(year_df, 1000), start=1):
                    block.to_excel(writer, sheet_name=f"{int(year)}_{idx}"[:31], index=False)

        for metric_name, df in metrics.items():
            df.to_excel(writer, sheet_name=metric_name[:31], index=False)


def build_metrics(master_df: pd.DataFrame, student_summary: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    return {
        "graduation_rates": calculate_graduation_rates(student_summary),
        "retention_rates": calculate_retention_rates(student_summary),
        "gpa_trends": calculate_gpa_trends(master_df),
        "credit_momentum": calculate_credit_momentum(master_df),
        "standing_distribution": calculate_standing_distribution(master_df),
    }


def main() -> None:
    ensure_directories()
    alias_lookup = build_alias_lookup()

    academic_raw = combine_sources([INBOX_ACADEMIC_DIR, RAW_ACADEMIC_DIR], "academic", alias_lookup)
    roster_raw = combine_sources([INBOX_ROSTER_DIR, RAW_ROSTER_DIR], "roster", alias_lookup)

    academic_df = normalize_academic_records(academic_raw)
    roster_df = normalize_roster_records(roster_raw)

    master_df, student_summary = build_master_dataset(academic_df, roster_df)
    metrics = build_metrics(master_df, student_summary)

    export_csv(master_df, PROCESSED_DIR / "master_dataset.csv")
    export_csv(student_summary, PROCESSED_DIR / "student_summary.csv")
    for metric_name, df in metrics.items():
        export_csv(df, METRICS_DIR / f"{metric_name}.csv")

    export_excel(master_df, metrics, EXCEL_DIR / "greek_life_master.xlsx")

    print("Pipeline completed.")
    print(f"Academic rows processed: {len(academic_df)}")
    print(f"Roster rows processed: {len(roster_df)}")
    print(f"Master rows written: {len(master_df)}")


if __name__ == "__main__":
    main()
