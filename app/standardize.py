from __future__ import annotations

from typing import Dict, Iterable, Optional

import numpy as np
import pandas as pd

from app.io_utils import (
    category_from_bool,
    coerce_numeric,
    normalize_text,
    parse_term_label,
)
from app.status_framework import build_outcome_resolution_fields
from src.build_current_snapshot_analytics import bucket_30_hours


def _text(frame: pd.DataFrame, column: str) -> pd.Series:
    if column not in frame.columns:
        return pd.Series("", index=frame.index, dtype="object")
    return frame[column].fillna("").astype(str).str.strip()


def _numeric(frame: pd.DataFrame, column: str) -> pd.Series:
    if column not in frame.columns:
        return pd.Series(np.nan, index=frame.index, dtype="float64")
    return coerce_numeric(frame[column])


def _flag(frame: pd.DataFrame, column: str) -> pd.Series:
    text = _text(frame, column).str.lower()
    return text.map(
        lambda value: True
        if value in {"yes", "y", "true", "1"}
        else False
        if value in {"no", "n", "false", "0"}
        else pd.NA
    )


def _last_non_blank(series: pd.Series) -> str:
    cleaned = series.fillna("").astype(str).str.strip()
    usable = cleaned.loc[cleaned.ne("")]
    return usable.iloc[-1] if not usable.empty else ""


def _build_status_bucket(value: object, status_code_map: Dict[str, Iterable[str]]) -> str:
    text = normalize_text(value).upper()
    if not text:
        return "Unknown"
    checks = {
        "Transfer": status_code_map.get("transfer", []),
        "Suspended": status_code_map.get("suspended", []),
        "Graduated": status_code_map.get("graduated", []),
        "Dropped/Inactive": status_code_map.get("inactive", []),
        "Active": status_code_map.get("active", []),
    }
    for bucket, tokens in checks.items():
        if any(token.upper() in text for token in tokens):
            return bucket
    if "GRAD" in text or "ALUM" in text:
        return "Graduated"
    if "TRANSFER" in text:
        return "Transfer"
    if "SUSPEND" in text:
        return "Suspended"
    if any(token in text for token in ["INACTIVE", "DROP", "RESIGN", "REVOK", "REMOVE"]):
        return "Dropped/Inactive"
    if any(token in text for token in ["ACTIVE", "MEMBER", "NEW MEMBER", "COUNCIL"]):
        return "Active"
    return normalize_text(value) or "Unknown"


def _band_label(value: float | int | None, bands: list[dict[str, object]]) -> str:
    if value is None or pd.isna(value):
        return "Unknown"
    number = float(value)
    for band in bands:
        lower = float(band.get("min", 0))
        upper = band.get("max")
        if number >= lower and (upper is None or number <= float(upper)):
            return str(band["label"])
    return "Unknown"


def _apply_chapter_mapping(frame: pd.DataFrame, chapter_column: str, chapter_mapping: pd.DataFrame) -> pd.DataFrame:
    result = frame.copy()
    for column in ["chapter_group", "council", "org_type", "family", "custom_group"]:
        if column not in result.columns:
            result[column] = ""

    if result.empty or chapter_mapping.empty:
        return result

    mapping = chapter_mapping.copy()
    mapping["_chapter_key"] = mapping["chapter"].fillna("").astype(str).str.strip().str.lower()
    result["_chapter_key"] = result[chapter_column].fillna("").astype(str).str.strip().str.lower()
    merged = result.merge(
        mapping[["_chapter_key", "chapter_group", "council", "org_type", "family", "custom_group"]],
        on="_chapter_key",
        how="left",
        suffixes=("", "_mapped"),
    )
    for column in ["chapter_group", "council", "org_type", "family", "custom_group"]:
        mapped = f"{column}_mapped"
        if mapped in merged.columns:
            merged[column] = merged[column].where(merged[column].fillna("").astype(str).str.strip().ne(""), merged[mapped])
            merged = merged.drop(columns=[mapped])
    return merged.drop(columns=["_chapter_key"])


def _finalize_summary(
    frame: pd.DataFrame,
    chapter_mapping: pd.DataFrame,
    settings: dict[str, object],
) -> pd.DataFrame:
    result = frame.copy()
    result["chapter"] = (
        result.get("initial_chapter", pd.Series("", index=result.index, dtype="object"))
        .fillna("")
        .astype(str)
        .str.strip()
    )
    result["chapter"] = result["chapter"].where(result["chapter"].ne(""), result.get("latest_chapter", ""))
    result["chapter"] = result["chapter"].fillna("").astype(str).str.strip()
    result["join_year"] = result["join_year"].where(result["join_year"].notna(), result["join_term"].map(lambda value: parse_term_label(value)["year"]))
    result["graduation_year"] = result["graduation_year"].where(
        result["graduation_year"].notna(),
        result["graduation_term"].map(lambda value: parse_term_label(value)["year"]),
    )
    result["chapter"] = result["chapter"].fillna("").astype(str).str.strip()
    result["is_fsl_member"] = result["chapter"].ne("")
    result = _apply_chapter_mapping(result, "chapter", chapter_mapping)
    result = result.drop(columns=[column for column in ["chapter_size", "chapter_size_band"] if column in result.columns])

    chapter_sizes = (
        result.loc[result["chapter"].ne("")]
        .groupby("chapter", dropna=False)["student_id"]
        .nunique()
        .rename("chapter_size")
    )
    result = result.merge(chapter_sizes, on="chapter", how="left")
    result["chapter_size_band"] = result["chapter_size"].map(
        lambda value: _band_label(value, settings.get("chapter_size_bands", []))
    )

    result["total_cumulative_hours"] = result["total_cumulative_hours"].where(
        result["total_cumulative_hours"].notna(),
        result["current_total_hours"],
    )
    result["estimated_join_stage"] = _text(result, "estimated_pre_org_stage_txst")
    result["estimated_join_stage"] = result["estimated_join_stage"].where(
        result["estimated_join_stage"].ne(""),
        _text(result, "entry_hours_bucket"),
    )
    result["estimated_join_stage"] = result["estimated_join_stage"].where(
        result["estimated_join_stage"].ne(""),
        result["entry_cumulative_hours"].map(bucket_30_hours),
    )
    result["high_hours_flag"] = result["total_cumulative_hours"].ge(settings.get("high_hours_threshold", 60))
    result["high_hours_group"] = result["high_hours_flag"].map(
        lambda value: category_from_bool(value, "High Hours", "Lower Hours")
    )
    result["active_membership_group"] = result["active_flag"].map(
        lambda value: category_from_bool(value, "Active", "Inactive/Other")
    )
    result["pell_group"] = result["pell_flag"].map(
        lambda value: category_from_bool(value, "Pell", "Non-Pell")
    )
    result["transfer_group"] = result["transfer_flag"].map(
        lambda value: category_from_bool(value, "Transfer", "Non-Transfer")
    )
    result["snapshot_group"] = result["snapshot_matched"].map(
        lambda value: category_from_bool(value, "Snapshot Matched", "No Snapshot Match")
    )
    result["status_group"] = result["latest_outcome_bucket"].fillna("").astype(str).str.strip().replace("", "Unknown")
    if "outcome_evidence_source" not in result.columns:
        result["outcome_evidence_source"] = ""
    outcome_resolution = build_outcome_resolution_fields(result, settings.get("outcome_resolution", {}))
    for column in outcome_resolution.columns:
        result[column] = outcome_resolution[column]
    if "graduation_status_without_evidence" in result.columns:
        corrected = result["graduation_status_without_evidence"].fillna(False).astype(bool)
        if "graduation_status_corrected_flag" not in result.columns:
            result["graduation_status_corrected_flag"] = ""
        if "graduation_status_correction_reason" not in result.columns:
            result["graduation_status_correction_reason"] = ""
        result.loc[corrected, "graduation_status_corrected_flag"] = "Yes"
        result.loc[
            corrected & result["graduation_status_correction_reason"].fillna("").astype(str).str.strip().eq(""),
            "graduation_status_correction_reason",
        ] = "Graduation claim was present, but no confirmed graduation evidence source was available."
        result.loc[corrected, "latest_outcome_bucket"] = "No Further Observation"
    confirmed_grad = result["is_graduated"].fillna(False).astype(bool) if "is_graduated" in result.columns else pd.Series(False, index=result.index)
    if "graduated_eventual" in result.columns:
        result.loc[~confirmed_grad, "graduated_eventual"] = False
    if "graduated_4yr" in result.columns:
        result.loc[~confirmed_grad, "graduated_4yr"] = False
    if "graduated_6yr" in result.columns:
        result.loc[~confirmed_grad, "graduated_6yr"] = False
    result["status_group"] = result["latest_outcome_bucket"].fillna("").astype(str).str.strip().replace("", "Unknown")
    result["major_group"] = result["major"].fillna("").astype(str).str.strip().replace("", "Unknown")
    result["chapter_group"] = result["chapter_group"].fillna("").astype(str).str.strip().replace("", "Unassigned")
    result["council"] = result["council"].fillna("").astype(str).str.strip().replace("", "Unknown")
    result["org_type"] = result["org_type"].fillna("").astype(str).str.strip().replace("", "Unknown")
    result["family"] = result["family"].fillna("").astype(str).str.strip().replace("", "Unknown")
    result["custom_group"] = result["custom_group"].fillna("").astype(str).str.strip().replace("", "Unassigned")
    completeness_fields = [
        field
        for field in settings.get("completeness_fields", [])
        if field in result.columns
    ]
    if completeness_fields:
        present = result[completeness_fields].notna() & result[completeness_fields].astype(str).ne("")
        result["data_completeness_rate"] = present.sum(axis=1) / len(completeness_fields)
    else:
        result["data_completeness_rate"] = np.nan
    return result


def _finalize_longitudinal(frame: pd.DataFrame, chapter_mapping: pd.DataFrame) -> pd.DataFrame:
    result = frame.copy()
    result = _apply_chapter_mapping(result, "chapter", chapter_mapping)
    result["observed_term"] = result["observed_term"].fillna("").astype(str).str.strip()
    result["observed_year"] = result["observed_year"].where(
        result["observed_year"].notna(),
        result["observed_term"].map(lambda value: parse_term_label(value)["year"]),
    )
    result["observed_term_sort"] = result["observed_term_sort"].where(
        result["observed_term_sort"].notna(),
        result["observed_term"].map(lambda value: parse_term_label(value)["sort_value"]),
    )
    result["major"] = result["major"].fillna("").astype(str).str.strip()
    result["chapter"] = result["chapter"].fillna("").astype(str).str.strip()
    result["chapter_group"] = result["chapter_group"].fillna("").astype(str).str.strip().replace("", "Unassigned")
    result["council"] = result["council"].fillna("").astype(str).str.strip().replace("", "Unknown")
    result["org_type"] = result["org_type"].fillna("").astype(str).str.strip().replace("", "Unknown")
    result["family"] = result["family"].fillna("").astype(str).str.strip().replace("", "Unknown")
    result["custom_group"] = result["custom_group"].fillna("").astype(str).str.strip().replace("", "Unassigned")
    return result


def merge_longitudinal_rollups(summary: pd.DataFrame, longitudinal: pd.DataFrame) -> pd.DataFrame:
    if summary.empty or longitudinal.empty or "student_id" not in longitudinal.columns:
        return summary

    ordered = longitudinal.sort_values(["student_id", "observed_term_sort"], na_position="last").copy()
    rollup = (
        ordered.groupby("student_id", dropna=False)
        .agg(
            average_term_gpa_from_long=("term_gpa", "mean"),
            latest_cumulative_hours_from_long=("cumulative_hours", "last"),
            latest_academic_standing_from_long=("academic_standing_bucket", _last_non_blank),
            latest_major_from_long=("major", _last_non_blank),
        )
        .reset_index()
    )

    merged = summary.merge(rollup, on="student_id", how="left")
    merged["average_term_gpa"] = merged["average_term_gpa"].where(
        merged["average_term_gpa"].notna(),
        merged["average_term_gpa_from_long"],
    )
    merged["total_cumulative_hours"] = merged["total_cumulative_hours"].where(
        merged["total_cumulative_hours"].notna(),
        merged["latest_cumulative_hours_from_long"],
    )
    merged["latest_academic_standing_bucket"] = merged["latest_academic_standing_bucket"].where(
        merged["latest_academic_standing_bucket"].fillna("").astype(str).str.strip().ne(""),
        merged["latest_academic_standing_from_long"],
    )
    merged["major"] = merged["major"].where(
        merged["major"].fillna("").astype(str).str.strip().ne(""),
        merged["latest_major_from_long"],
    )
    return merged.drop(
        columns=[
            "average_term_gpa_from_long",
            "latest_cumulative_hours_from_long",
            "latest_academic_standing_from_long",
            "latest_major_from_long",
        ]
    )


def standardize_enhanced_summary(
    summary: pd.DataFrame,
    chapter_mapping: pd.DataFrame,
    settings: dict[str, object],
) -> pd.DataFrame:
    frame = pd.DataFrame(index=summary.index)
    frame["student_id"] = _text(summary, "Student ID")
    frame["student_name"] = (_text(summary, "Preferred First Name") + " " + _text(summary, "Preferred Last Name")).str.strip()
    frame["initial_chapter"] = _text(summary, "Initial Chapter")
    frame["latest_chapter"] = _text(summary, "Latest Chapter")
    frame["join_term"] = _text(summary, "First Observed Organization Term")
    frame["join_year"] = frame["join_term"].map(lambda value: parse_term_label(value)["year"])
    frame["cohort_label"] = _text(summary, "Organization Entry Cohort")
    frame["latest_outcome_bucket"] = _text(summary, "Latest Known Outcome Bucket").replace("", "Unknown")
    frame["latest_roster_status_bucket"] = _text(summary, "Latest Known Roster Status Bucket").replace("", "Unknown")
    frame["initial_roster_status_bucket"] = _text(summary, "Initial Roster Status Bucket").replace("", "Unknown")
    frame["active_flag"] = frame["latest_roster_status_bucket"].eq("Active")
    frame["major"] = _text(summary, "Latest Major").replace("", pd.NA).fillna(_text(summary, "First Major After Org Entry"))
    frame["pell_flag"] = pd.Series(pd.NA, index=summary.index, dtype="object")
    frame["transfer_flag"] = pd.Series(pd.NA, index=summary.index, dtype="object")
    frame["graduation_term"] = _text(summary, "Observed Graduation Term")
    frame["graduation_year"] = frame["graduation_term"].map(lambda value: parse_term_label(value)["year"])
    frame["entry_cumulative_hours"] = _numeric(summary, "Entry Cumulative Hours")
    frame["entry_hours_bucket"] = _text(summary, "Entry Cumulative Hours Bucket")
    frame["estimated_pre_org_hours_txst"] = pd.Series(np.nan, index=summary.index)
    frame["estimated_pre_org_stage_txst"] = _text(summary, "Entry Cumulative Hours Bucket")
    frame["current_total_hours"] = pd.Series(np.nan, index=summary.index)
    frame["total_cumulative_hours"] = pd.Series(np.nan, index=summary.index)
    frame["first_term_gpa"] = _numeric(summary, "First Post-Entry Term GPA")
    frame["second_term_gpa"] = _numeric(summary, "Second Post-Entry Term GPA")
    frame["first_year_avg_term_gpa"] = _numeric(summary, "First-Year Average Term GPA After Org Entry")
    frame["average_term_gpa"] = frame["first_year_avg_term_gpa"]
    frame["gpa_change"] = _numeric(summary, "Change In Term GPA First To Second Term")
    frame["latest_overall_cumulative_gpa"] = _numeric(summary, "Latest Overall Cumulative GPA")
    frame["latest_txstate_cumulative_gpa"] = _numeric(summary, "Latest TxState Cumulative GPA")
    frame["average_cumulative_gpa"] = frame["latest_overall_cumulative_gpa"].where(
        frame["latest_overall_cumulative_gpa"].notna(),
        frame["latest_txstate_cumulative_gpa"],
    )
    frame["first_term_passed_hours"] = _numeric(summary, "First Post-Entry Passed Hours")
    frame["first_year_passed_hours"] = _numeric(summary, "First-Year Passed Hours After Org Entry")
    frame["graduated_eventual"] = _flag(summary, "Eventual Observed Graduation From Org Entry")
    frame["graduated_eventual_measurable"] = frame["join_term"].ne("")
    frame["graduated_4yr"] = _flag(summary, "Observed Graduation Within 4 Years Of Org Entry")
    frame["graduated_4yr_measurable"] = _flag(summary, "Observed Graduation Within 4 Years Of Org Entry Measurable")
    frame["graduated_6yr"] = _flag(summary, "Observed Graduation Within 6 Years Of Org Entry")
    frame["graduated_6yr_measurable"] = _flag(summary, "Observed Graduation Within 6 Years Of Org Entry Measurable")
    frame["outcome_evidence_source"] = pd.Series("", index=summary.index, dtype="object")
    frame.loc[frame["graduation_term"].fillna("").astype(str).str.strip().ne(""), "outcome_evidence_source"] = "Observed graduation term"
    frame.loc[
        frame["outcome_evidence_source"].eq("") & frame["graduated_eventual"].fillna(False).astype(bool),
        "outcome_evidence_source",
    ] = "Enhanced graduation flag"
    frame["retained_next_term"] = _flag(summary, "Retained In Organization To Next Observed Term")
    frame["retained_next_term_measurable"] = _flag(summary, "Organization Next Observed Term Measurable")
    frame["retained_next_fall"] = _flag(summary, "Retained In Organization To Next Fall")
    frame["retained_next_fall_measurable"] = _flag(summary, "Organization Next Fall Measurable")
    frame["retained_one_year"] = _flag(summary, "Retained In Organization One Year After Entry")
    frame["retained_one_year_measurable"] = _flag(summary, "Organization One-Year Same-Season Measurable")
    frame["continued_next_term"] = _flag(summary, "Continued Academically To Next Observed Term")
    frame["continued_next_term_measurable"] = _flag(summary, "Academic Next Observed Term Measurable")
    frame["continued_next_fall"] = _flag(summary, "Continued Academically To Next Fall")
    frame["continued_next_fall_measurable"] = _flag(summary, "Academic Next Fall Measurable")
    frame["continued_one_year"] = _flag(summary, "Continued Academically One Year After Entry")
    frame["continued_one_year_measurable"] = _flag(summary, "Academic One-Year Same-Season Measurable")
    frame["low_gpa_2_0_flag"] = _flag(summary, "First-Term GPA Below 2.0 Flag")
    frame["low_gpa_2_5_flag"] = _flag(summary, "First-Term GPA Below 2.5 Flag")
    frame["first_year_low_gpa_2_0_flag"] = _flag(summary, "First-Year Average GPA Below 2.0 Flag")
    frame["first_year_low_gpa_2_5_flag"] = _flag(summary, "First-Year Average GPA Below 2.5 Flag")
    frame["good_standing_first_term"] = _flag(summary, "First-Term Good Standing Flag")
    frame["probation_warning_first_year"] = _flag(summary, "First-Year Probation/Warning Flag")
    frame["academic_standing_suspended_ever"] = _flag(summary, "Academic Standing Suspended Ever Flag")
    frame["first_academic_standing_bucket"] = _text(summary, "First Academic Standing After Org Entry").replace("", "Unknown")
    frame["latest_academic_standing_bucket"] = _text(summary, "Latest Academic Standing").replace("", "Unknown")
    frame["snapshot_matched"] = pd.Series(pd.NA, index=summary.index, dtype="object")
    frame["source_logic"] = "enhanced_bundle"
    return _finalize_summary(frame, chapter_mapping, settings)


def standardize_snapshot_summary(
    summary: pd.DataFrame,
    chapter_mapping: pd.DataFrame,
    settings: dict[str, object],
) -> pd.DataFrame:
    frame = standardize_enhanced_summary(summary, chapter_mapping, settings)
    augmented_outcome = _text(summary, "Augmented Latest Outcome Bucket")
    frame["latest_outcome_bucket"] = augmented_outcome.where(augmented_outcome.ne(""), frame["latest_outcome_bucket"])
    frame["graduated_eventual"] = _flag(summary, "Augmented Ever Graduated Flag")
    frame.loc[
        frame["graduated_eventual"].fillna(False).astype(bool),
        "outcome_evidence_source",
    ] = "Snapshot augmented graduation flag"
    frame["snapshot_matched"] = _flag(summary, "Snapshot Matched")
    frame["current_total_hours"] = _numeric(summary, "Snapshot Total Credit Hours")
    frame["total_cumulative_hours"] = frame["current_total_hours"]
    frame["current_txst_hours"] = _numeric(summary, "Snapshot TXST Credit Hours")
    frame["estimated_pre_org_hours_txst"] = _numeric(summary, "Estimated Pre-Org Credit Hours (TXST Basis)")
    frame["estimated_pre_org_stage_txst"] = _text(summary, "Estimated Pre-Org Stage (TXST Basis)")
    frame["average_cumulative_gpa"] = frame["average_cumulative_gpa"].where(
        frame["average_cumulative_gpa"].notna(),
        _numeric(summary, "Snapshot Overall GPA").where(
            _numeric(summary, "Snapshot Overall GPA").notna(),
            _numeric(summary, "Snapshot Institutional GPA"),
        ),
    )
    frame["source_logic"] = "current_snapshot_augmented_bundle"
    return _finalize_summary(frame, chapter_mapping, settings)


def standardize_processed_summary(
    summary: pd.DataFrame,
    chapter_mapping: pd.DataFrame,
    settings: dict[str, object],
    status_code_map: Dict[str, Iterable[str]],
) -> pd.DataFrame:
    frame = pd.DataFrame(index=summary.index)
    frame["student_id"] = _text(summary, "student_id")
    frame["student_name"] = (_text(summary, "first_name") + " " + _text(summary, "last_name")).str.strip()
    frame["initial_chapter"] = _text(summary, "chapter")
    frame["latest_chapter"] = _text(summary, "chapter")
    frame["join_term"] = _text(summary, "join_term")
    frame["join_year"] = frame["join_term"].map(lambda value: parse_term_label(value)["year"])
    frame["cohort_label"] = frame["join_term"].where(frame["join_term"].ne(""), _text(summary, "first_term"))
    latest_status = _text(summary, "latest_membership_status")
    status_bucket = latest_status.map(lambda value: _build_status_bucket(value, status_code_map))
    frame["latest_outcome_bucket"] = status_bucket
    frame["latest_roster_status_bucket"] = status_bucket
    frame["initial_roster_status_bucket"] = status_bucket
    frame["active_flag"] = status_bucket.eq("Active")
    frame["major"] = _text(summary, "major")
    pell_text = _text(summary, "pell_flag").str.lower()
    frame["pell_flag"] = pell_text.map(
        lambda value: True if value in {"yes", "y", "true", "1"} else False if value in {"no", "n", "false", "0"} else pd.NA
    )
    cohort_text = _text(summary, "cohort").str.lower()
    frame["transfer_flag"] = cohort_text.map(
        lambda value: True if "transfer" in value else False if value else pd.NA
    )
    frame["graduation_term"] = pd.Series("", index=summary.index, dtype="object")
    frame["graduation_year"] = pd.Series(np.nan, index=summary.index)
    frame["entry_cumulative_hours"] = pd.Series(np.nan, index=summary.index)
    frame["entry_hours_bucket"] = pd.Series("", index=summary.index, dtype="object")
    frame["estimated_pre_org_hours_txst"] = pd.Series(np.nan, index=summary.index)
    frame["estimated_pre_org_stage_txst"] = pd.Series("", index=summary.index, dtype="object")
    frame["current_total_hours"] = _numeric(summary, "total_earned")
    frame["total_cumulative_hours"] = _numeric(summary, "total_earned")
    frame["first_term_gpa"] = pd.Series(np.nan, index=summary.index)
    frame["second_term_gpa"] = pd.Series(np.nan, index=summary.index)
    frame["first_year_avg_term_gpa"] = pd.Series(np.nan, index=summary.index)
    frame["average_term_gpa"] = _numeric(summary, "avg_term_gpa")
    frame["gpa_change"] = pd.Series(np.nan, index=summary.index)
    frame["latest_overall_cumulative_gpa"] = _numeric(summary, "latest_gpa_cum")
    frame["latest_txstate_cumulative_gpa"] = pd.Series(np.nan, index=summary.index)
    frame["average_cumulative_gpa"] = _numeric(summary, "latest_gpa_cum")
    frame["first_term_passed_hours"] = pd.Series(np.nan, index=summary.index)
    frame["first_year_passed_hours"] = pd.Series(np.nan, index=summary.index)
    frame["graduated_eventual"] = _flag(summary, "graduated")
    frame["graduated_eventual_measurable"] = _text(summary, "student_id").ne("")
    frame["graduated_4yr"] = _flag(summary, "graduated_4yr")
    frame["graduated_4yr_measurable"] = _text(summary, "first_term").ne("")
    frame["graduated_6yr"] = _flag(summary, "graduated_6yr")
    frame["graduated_6yr_measurable"] = _text(summary, "first_term").ne("")
    frame["outcome_evidence_source"] = pd.Series("", index=summary.index, dtype="object")
    frame.loc[frame["graduated_eventual"].fillna(False).astype(bool), "outcome_evidence_source"] = "Processed graduation flag"
    frame["retained_next_term"] = pd.Series(pd.NA, index=summary.index, dtype="object")
    frame["retained_next_term_measurable"] = pd.Series(pd.NA, index=summary.index, dtype="object")
    frame["retained_next_fall"] = pd.Series(pd.NA, index=summary.index, dtype="object")
    frame["retained_next_fall_measurable"] = pd.Series(pd.NA, index=summary.index, dtype="object")
    frame["retained_one_year"] = (_numeric(summary, "last_term_sort") - _numeric(summary, "first_term_sort")).ge(10)
    frame["retained_one_year_measurable"] = _text(summary, "first_term").ne("")
    frame["continued_next_term"] = pd.Series(pd.NA, index=summary.index, dtype="object")
    frame["continued_next_term_measurable"] = pd.Series(pd.NA, index=summary.index, dtype="object")
    frame["continued_next_fall"] = pd.Series(pd.NA, index=summary.index, dtype="object")
    frame["continued_next_fall_measurable"] = pd.Series(pd.NA, index=summary.index, dtype="object")
    frame["continued_one_year"] = frame["retained_one_year"]
    frame["continued_one_year_measurable"] = frame["retained_one_year_measurable"]
    frame["low_gpa_2_0_flag"] = pd.Series(pd.NA, index=summary.index, dtype="object")
    frame["low_gpa_2_5_flag"] = pd.Series(pd.NA, index=summary.index, dtype="object")
    frame["first_year_low_gpa_2_0_flag"] = pd.Series(pd.NA, index=summary.index, dtype="object")
    frame["first_year_low_gpa_2_5_flag"] = pd.Series(pd.NA, index=summary.index, dtype="object")
    frame["good_standing_first_term"] = pd.Series(pd.NA, index=summary.index, dtype="object")
    frame["probation_warning_first_year"] = pd.Series(pd.NA, index=summary.index, dtype="object")
    frame["academic_standing_suspended_ever"] = pd.Series(pd.NA, index=summary.index, dtype="object")
    frame["first_academic_standing_bucket"] = pd.Series("Unknown", index=summary.index, dtype="object")
    frame["latest_academic_standing_bucket"] = pd.Series("Unknown", index=summary.index, dtype="object")
    frame["snapshot_matched"] = pd.Series(pd.NA, index=summary.index, dtype="object")
    frame["source_logic"] = "processed_pipeline"
    return _finalize_summary(frame, chapter_mapping, settings)


def standardize_enhanced_longitudinal(
    longitudinal: pd.DataFrame,
    chapter_mapping: pd.DataFrame,
) -> pd.DataFrame:
    frame = pd.DataFrame(index=longitudinal.index)
    frame["student_id"] = _text(longitudinal, "Student ID")
    frame["chapter"] = _text(longitudinal, "Chapter")
    frame["observed_term"] = _text(longitudinal, "Term")
    frame["observed_year"] = _numeric(longitudinal, "Term Year")
    frame["observed_term_sort"] = _text(longitudinal, "Term Code").map(lambda value: parse_term_label(value)["sort_value"])
    frame["join_term"] = _text(longitudinal, "Organization Entry Term")
    frame["join_year"] = frame["join_term"].map(lambda value: parse_term_label(value)["year"])
    frame["relative_term_index"] = _numeric(longitudinal, "Relative Term Index From Org Entry")
    frame["major"] = _text(longitudinal, "Major")
    frame["term_gpa"] = _numeric(longitudinal, "Term GPA")
    frame["txstate_cumulative_gpa"] = _numeric(longitudinal, "TxState Cumulative GPA")
    frame["overall_cumulative_gpa"] = _numeric(longitudinal, "Overall Cumulative GPA")
    frame["cumulative_gpa"] = frame["overall_cumulative_gpa"].where(
        frame["overall_cumulative_gpa"].notna(),
        frame["txstate_cumulative_gpa"],
    )
    frame["cumulative_hours"] = _numeric(longitudinal, "Cumulative Hours")
    frame["term_passed_hours"] = _numeric(longitudinal, "Term Passed Hours")
    frame["semester_hours"] = _numeric(longitudinal, "Semester Hours")
    frame["academic_standing_bucket"] = _text(longitudinal, "Academic Standing Bucket").replace("", "Unknown")
    frame["roster_present"] = _flag(longitudinal, "Roster Present")
    frame["academic_present"] = _flag(longitudinal, "Academic Present")
    frame["active_flag"] = _text(longitudinal, "Roster Status Bucket").eq("Active")
    frame["source_logic"] = "enhanced_bundle"
    return _finalize_longitudinal(frame, chapter_mapping)


def standardize_processed_longitudinal(
    longitudinal: pd.DataFrame,
    chapter_mapping: pd.DataFrame,
) -> pd.DataFrame:
    frame = pd.DataFrame(index=longitudinal.index)
    frame["student_id"] = _text(longitudinal, "student_id")
    frame["chapter"] = _text(longitudinal, "chapter")
    frame["observed_term"] = _text(longitudinal, "term")
    frame["observed_year"] = _numeric(longitudinal, "year")
    frame["observed_term_sort"] = _numeric(longitudinal, "term_sort")
    frame["join_term"] = _text(longitudinal, "join_term")
    frame["join_year"] = frame["join_term"].map(lambda value: parse_term_label(value)["year"])
    frame["relative_term_index"] = _numeric(longitudinal, "time_in_greek")
    frame["major"] = _text(longitudinal, "major")
    frame["term_gpa"] = _numeric(longitudinal, "gpa_term")
    frame["txstate_cumulative_gpa"] = pd.Series(np.nan, index=longitudinal.index)
    frame["overall_cumulative_gpa"] = _numeric(longitudinal, "gpa_cum")
    frame["cumulative_gpa"] = _numeric(longitudinal, "gpa_cum")
    frame["cumulative_hours"] = pd.Series(np.nan, index=longitudinal.index)
    frame["term_passed_hours"] = _numeric(longitudinal, "credits_earned")
    frame["semester_hours"] = _numeric(longitudinal, "credits_attempted")
    frame["academic_standing_bucket"] = _text(longitudinal, "academic_standing").replace("", "Unknown")
    frame["roster_present"] = _text(longitudinal, "chapter").ne("")
    frame["academic_present"] = pd.Series(True, index=longitudinal.index)
    frame["active_flag"] = longitudinal.get("active_by_term", pd.Series(False, index=longitudinal.index)).fillna(False).astype(bool)
    frame["source_logic"] = "processed_pipeline"
    return _finalize_longitudinal(frame, chapter_mapping)
