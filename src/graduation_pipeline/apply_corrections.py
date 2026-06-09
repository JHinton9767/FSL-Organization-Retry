from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.build_master_roster import normalize_banner_id, normalize_chapter_name
from src.shared_utils import clean_text

from .config import MANUAL_CORRECTION_COLUMNS
from .normalize import normalize_term


def ensure_manual_corrections_file(path: Path) -> None:
    if path.exists():
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(columns=MANUAL_CORRECTION_COLUMNS).to_csv(path, index=False)


def load_manual_corrections(path: Path) -> pd.DataFrame:
    ensure_manual_corrections_file(path)
    try:
        corrections = pd.read_csv(path, dtype=str, keep_default_na=False)
    except Exception:
        corrections = pd.DataFrame(columns=MANUAL_CORRECTION_COLUMNS)
    for column in MANUAL_CORRECTION_COLUMNS:
        if column not in corrections.columns:
            corrections[column] = ""
    corrections = corrections[MANUAL_CORRECTION_COLUMNS].copy()
    corrections["student_id"] = corrections["banner_id"].map(normalize_banner_id)
    corrections = corrections.loc[corrections["student_id"].ne("")].copy()
    if "active" in corrections.columns:
        active = corrections["active"].map(lambda value: clean_text(value).lower())
        corrections = corrections.loc[active.isin({"", "y", "yes", "true", "1", "active"})].copy()
    corrections["corrected_chapter"] = corrections["corrected_chapter"].map(lambda value: normalize_chapter_name(clean_text(value)))
    corrections["corrected_graduation_status"] = corrections["corrected_graduation_status"].map(clean_text)
    corrections["corrected_graduation_term_code"] = corrections["corrected_graduation_term"].map(lambda value: normalize_term(value)[0])
    corrections["corrected_first_fsl_term_code"] = corrections["corrected_first_fsl_term"].map(lambda value: normalize_term(value)[0])
    return corrections.reset_index(drop=True)


def latest_correction_by_student(corrections: pd.DataFrame) -> pd.DataFrame:
    if corrections.empty:
        return corrections
    corrections = corrections.copy()
    corrections["_reviewed_sort"] = corrections["reviewed_date"].map(clean_text)
    return corrections.sort_values(["student_id", "_reviewed_sort"]).drop_duplicates("student_id", keep="last")

