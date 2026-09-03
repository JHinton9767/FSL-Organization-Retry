from __future__ import annotations

import csv
import hashlib
import re
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable, Mapping


VALID_BANNER_ID_RE = re.compile(r"^A0\d{7}$")
TERM_CODE_RE = re.compile(r"^(19\d{2}|20\d{2})(WI|SP|SU|FA|UN)$", re.IGNORECASE)
TERM_RE = re.compile(r"\b(Winter|Spring|Summer|Fall)\s+(19\d{2}|20\d{2})\b", re.IGNORECASE)
SEASON_ORDER = {"WI": 0, "SP": 1, "SU": 2, "FA": 3, "UN": 9}
SEASON_NAME = {"WI": "Winter", "SP": "Spring", "SU": "Summer", "FA": "Fall", "UN": "Unknown"}

STATUS_COUNT_COLUMNS = [
    ("active_count", "Active"),
    ("new_member_count", "New Member"),
    ("early_alumni_count", "Early Alumni"),
    ("graduated_count", "Graduated"),
    ("inactive_suspended_count", "Inactive/Suspended"),
    ("dropped_resigned_count", "Dropped/Resigned"),
    ("revoked_count", "Revoked"),
    ("transfer_count", "Transfer"),
    ("chapter_kicked_count", "Chapter Kicked"),
    ("h_count", "H"),
    ("unknown_status_count", "Unknown"),
]
STATUS_PRIORITY = {status: index for index, (_, status) in enumerate(STATUS_COUNT_COLUMNS)}
STATUS_PRIORITY.update({"Unknown": -1})

INVENTORY_COLUMNS = [
    "term_code",
    "term_label",
    "term_sort",
    "chapter",
    "unique_valid_banner_ids",
    "active_or_new_count",
    *[column for column, _ in STATUS_COUNT_COLUMNS],
    "status_buckets_seen",
    "source_file_count",
    "source_files",
    "valid_roster_rows_seen",
    "duplicate_student_rows_in_term_chapter",
]

LIFECYCLE_COLUMNS = [
    "chapter",
    "first_seen_term",
    "last_seen_term",
    "terms_present_count",
    "total_unique_valid_banner_ids_seen",
    "possible_gap_count_between_first_and_last_seen",
    "possible_roster_gaps_between_first_and_last_seen",
    "kicked_off_term",
    "returned_term",
    "notes",
]

CHAPTER_STATUS_EVENT_CANDIDATE_COLUMNS = [
    "candidate_key",
    "chapter",
    "candidate_event_type",
    "suggested_event_type",
    "confidence",
    "review_status",
    "first_seen_term",
    "last_seen_term",
    "last_seen_before_gap",
    "missing_start_term",
    "missing_end_term",
    "returned_term",
    "terms_missing_count",
    "missing_terms",
    "terms_present_count",
    "total_unique_valid_banner_ids_seen",
    "last_seen_unique_valid_banner_ids",
    "last_seen_active_or_new_count",
    "source_files_last_seen",
    "evidence_source",
    "evidence_file_or_url",
    "evidence_summary",
    "notes",
]


@dataclass
class ChapterTermPresence:
    term_code: str
    term_label: str
    term_sort: int
    chapter: str
    student_statuses: dict[str, str] = field(default_factory=dict)
    source_files: set[str] = field(default_factory=set)
    valid_row_count: int = 0

    def add(self, student_id: str, status: str, source_file: str) -> None:
        self.valid_row_count += 1
        current = self.student_statuses.get(student_id, "Unknown")
        if STATUS_PRIORITY.get(status, -1) >= STATUS_PRIORITY.get(current, -1):
            self.student_statuses[student_id] = status
        if source_file:
            self.source_files.add(source_file)

    @property
    def duplicate_student_rows(self) -> int:
        return max(0, self.valid_row_count - len(self.student_statuses))


@dataclass(frozen=True)
class ChapterSemesterTables:
    inventory_rows: list[dict[str, object]]
    matrix_columns: list[str]
    matrix_rows: list[dict[str, object]]
    lifecycle_rows: list[dict[str, object]]
    status_event_candidate_rows: list[dict[str, object]]
    term_count: int
    chapter_count: int
    source_rows: int
    valid_rows: int
    invalid_id_rows: int
    excluded_position_rows: int


@dataclass(frozen=True)
class ChapterSemesterExportResult:
    inventory_path: Path
    matrix_path: Path
    lifecycle_review_path: Path
    status_event_candidates_path: Path
    inventory_rows: int
    matrix_rows: int
    lifecycle_rows: int
    status_event_candidate_rows: int
    term_count: int
    chapter_count: int
    source_rows: int
    valid_rows: int
    invalid_id_rows: int
    excluded_position_rows: int


def clean_text(value: object) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    return "" if text.lower() in {"nan", "none", "<na>"} else text


def normalize_banner_id(value: object) -> str:
    text = clean_text(value).upper()
    if text.endswith(".0"):
        text = text[:-2]
    text = re.sub(r"\s+", "", text)
    return text if VALID_BANNER_ID_RE.fullmatch(text) else ""


def source_file_name(value: object) -> str:
    text = clean_text(value)
    if not text:
        return ""
    return text.replace("\\", "/").rstrip("/").split("/")[-1]


def parse_term(value: object) -> tuple[str, str, int]:
    text = clean_text(value)
    match = TERM_CODE_RE.fullmatch(text.upper())
    if match:
        year = int(match.group(1))
        season_code = match.group(2).upper()
        label = str(year) if season_code == "UN" else f"{SEASON_NAME[season_code]} {year}"
        return f"{year}{season_code}", label, year * 10 + SEASON_ORDER.get(season_code, 9)

    match = TERM_RE.search(text)
    if match:
        season_name = match.group(1).title()
        year = int(match.group(2))
        season_code = next(code for code, label in SEASON_NAME.items() if label == season_name)
        return f"{year}{season_code}", f"{season_name} {year}", year * 10 + SEASON_ORDER[season_code]

    year_match = re.search(r"(19\d{2}|20\d{2})", text)
    if year_match:
        year = int(year_match.group(1))
        return f"{year}UN", str(year), year * 10 + SEASON_ORDER["UN"]

    return "", text, 999999


def term_from_row(row: Mapping[str, object]) -> tuple[str, str, int]:
    term_code, term_label, term_sort = parse_term(row.get("term_code", ""))
    _, label_label, label_sort = parse_term(row.get("term_label", ""))
    if term_code and label_label and label_sort != 999999:
        return term_code, label_label, term_sort
    if term_code:
        return term_code, term_label, term_sort
    return parse_term(row.get("term_label", ""))


def is_excluded_position(value: object) -> bool:
    text = clean_text(value).lower()
    return bool(text and ("advisor" in text or "greek staff" in text))


def normalize_status(bucket_value: object, raw_value: object = "") -> str:
    text = clean_text(bucket_value) or clean_text(raw_value)
    compact = re.sub(r"[^A-Z0-9]+", "", text.upper())
    lowered = re.sub(r"\s+", " ", text.lower()).strip()

    if not compact:
        return "Unknown"
    if compact in {"A", "ACTIVE", "ACTIVEMEMBER"}:
        return "Active"
    if compact in {"N", "NEW", "NEWMEMBER"}:
        return "New Member"
    if compact in {"AL", "ALUMNI", "EARLYALUMNI"}:
        return "Early Alumni"
    if compact in {"G", "GRAD", "GRADUATED"}:
        return "Graduated"
    if compact in {"I", "INACTIVE", "S", "SUSPEND", "SUSPENDED", "IS", "INACTIVESUSPEND", "INACTIVESUSPENDED"}:
        return "Inactive/Suspended"
    if compact in {"RS", "RESIGNED", "DROPPED", "DROPOUT", "DROPPEDRESIGNED"}:
        return "Dropped/Resigned"
    if compact in {"RV", "REVOKED"}:
        return "Revoked"
    if compact in {"T", "TRANSFER", "TRANSFERRED", "LEFTINSTITUTION"}:
        return "Transfer"
    if compact == "H":
        return "H"
    if "chapter kicked" in lowered or compact == "CHAPTERKICKED":
        return "Chapter Kicked"
    if "unknown" in lowered or "unresolved" in lowered:
        return "Unknown"
    return "Unknown"


def status_counts_for(presence: ChapterTermPresence) -> Counter[str]:
    counts: Counter[str] = Counter()
    for status in presence.student_statuses.values():
        counts[status] += 1
    return counts


def status_summary(counts: Counter[str]) -> str:
    parts = [f"{status}={counts[status]}" for _, status in STATUS_COUNT_COLUMNS if counts.get(status, 0)]
    return " | ".join(parts)


def sorted_term_keys(presence_by_key: dict[tuple[str, str], ChapterTermPresence]) -> list[tuple[str, str, int, str]]:
    seen: dict[str, tuple[str, int, str]] = {}
    for presence in presence_by_key.values():
        term_key = presence.term_code or presence.term_label
        candidate = (presence.term_label, presence.term_sort, presence.term_code)
        current = seen.get(term_key)
        if current is None or candidate[1] < current[1]:
            seen[term_key] = candidate
    return sorted(
        [(term_key, label, sort_value, code) for term_key, (label, sort_value, code) in seen.items()],
        key=lambda item: (item[2], item[1]),
    )


def chapter_sort_key(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", clean_text(value).lower()).strip()


def inventory_rows(presence_by_key: dict[tuple[str, str], ChapterTermPresence]) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for presence in sorted(presence_by_key.values(), key=lambda item: (item.term_sort, chapter_sort_key(item.chapter))):
        counts = status_counts_for(presence)
        row = {
            "term_code": presence.term_code,
            "term_label": presence.term_label,
            "term_sort": presence.term_sort,
            "chapter": presence.chapter,
            "unique_valid_banner_ids": len(presence.student_statuses),
            "active_or_new_count": counts.get("Active", 0) + counts.get("New Member", 0),
            "status_buckets_seen": status_summary(counts),
            "source_file_count": len(presence.source_files),
            "source_files": " | ".join(sorted(presence.source_files)),
            "valid_roster_rows_seen": presence.valid_row_count,
            "duplicate_student_rows_in_term_chapter": presence.duplicate_student_rows,
        }
        for column, status in STATUS_COUNT_COLUMNS:
            row[column] = counts.get(status, 0)
        rows.append(row)
    return rows


def matrix_rows(
    presence_by_key: dict[tuple[str, str], ChapterTermPresence],
    terms: list[tuple[str, str, int, str]],
) -> tuple[list[str], list[dict[str, object]]]:
    term_labels = [label for _, label, _, _ in terms]
    chapters = sorted({presence.chapter for presence in presence_by_key.values()}, key=chapter_sort_key)
    term_lookup = {term_key: label for term_key, label, _, _ in terms}
    rows: list[dict[str, object]] = []
    for chapter in chapters:
        row: dict[str, object] = {"chapter": chapter}
        for label in term_labels:
            row[label] = ""
        for (term_key, presence_chapter), presence in presence_by_key.items():
            if presence_chapter == chapter:
                row[term_lookup[term_key]] = len(presence.student_statuses)
        rows.append(row)
    return ["chapter", *term_labels], rows


def lifecycle_rows(
    presence_by_key: dict[tuple[str, str], ChapterTermPresence],
    terms: list[tuple[str, str, int, str]],
) -> list[dict[str, object]]:
    term_labels = [label for _, label, _, _ in terms]
    term_sorts = {label: sort_value for _, label, sort_value, _ in terms}
    chapters = sorted({presence.chapter for presence in presence_by_key.values()}, key=chapter_sort_key)
    rows: list[dict[str, object]] = []
    for chapter in chapters:
        chapter_presence = [presence for presence in presence_by_key.values() if presence.chapter == chapter]
        present_labels = {presence.term_label for presence in chapter_presence}
        sorted_present = sorted(present_labels, key=lambda label: term_sorts.get(label, 999999))
        first_seen = sorted_present[0] if sorted_present else ""
        last_seen = sorted_present[-1] if sorted_present else ""
        if first_seen and last_seen:
            first_sort = term_sorts.get(first_seen, 999999)
            last_sort = term_sorts.get(last_seen, 999999)
            expected_terms = [label for label in term_labels if first_sort <= term_sorts.get(label, 999999) <= last_sort]
            missing_terms = [label for label in expected_terms if label not in present_labels]
        else:
            missing_terms = []

        all_student_ids: set[str] = set()
        for presence in chapter_presence:
            all_student_ids.update(presence.student_statuses)

        rows.append(
            {
                "chapter": chapter,
                "first_seen_term": first_seen,
                "last_seen_term": last_seen,
                "terms_present_count": len(present_labels),
                "total_unique_valid_banner_ids_seen": len(all_student_ids),
                "possible_gap_count_between_first_and_last_seen": len(missing_terms),
                "possible_roster_gaps_between_first_and_last_seen": " | ".join(missing_terms),
                "kicked_off_term": "",
                "returned_term": "",
                "notes": "",
            }
        )
    return rows


def chapter_status_event_candidate_rows(
    presence_by_key: dict[tuple[str, str], ChapterTermPresence],
    terms: list[tuple[str, str, int, str]],
) -> list[dict[str, object]]:
    term_labels = [label for _, label, _, _ in terms]
    term_key_by_label = {label: term_key for term_key, label, _, _ in terms}
    term_index_by_label = {label: index for index, label in enumerate(term_labels)}
    chapters = sorted({presence.chapter for presence in presence_by_key.values()}, key=chapter_sort_key)
    rows: list[dict[str, object]] = []

    for chapter in chapters:
        chapter_presence = [presence for presence in presence_by_key.values() if presence.chapter == chapter]
        present_labels = {presence.term_label for presence in chapter_presence}
        present_indices = sorted(term_index_by_label[label] for label in present_labels if label in term_index_by_label)
        if not present_indices:
            continue

        all_student_ids: set[str] = set()
        for presence in chapter_presence:
            all_student_ids.update(presence.student_statuses)

        first_seen = term_labels[present_indices[0]]
        last_seen = term_labels[present_indices[-1]]
        terms_present_count = len(present_indices)
        idx = present_indices[0]
        present_index_set = set(present_indices)
        while idx < len(term_labels):
            if idx in present_index_set:
                idx += 1
                continue

            gap_start = idx
            while idx < len(term_labels) and idx not in present_index_set:
                idx += 1
            gap_end = idx - 1
            previous_present_indices = [value for value in present_indices if value < gap_start]
            if not previous_present_indices:
                continue

            last_seen_before_gap = term_labels[previous_present_indices[-1]]
            returned_term = term_labels[idx] if idx < len(term_labels) and idx in present_index_set else ""
            missing_terms = term_labels[gap_start : gap_end + 1]
            if not missing_terms:
                continue

            last_seen_key = term_key_by_label.get(last_seen_before_gap, "")
            last_presence = presence_by_key.get((last_seen_key, chapter))
            counts = status_counts_for(last_presence) if last_presence else Counter()
            source_files = " | ".join(sorted(last_presence.source_files)) if last_presence else ""
            active_or_new = counts.get("Active", 0) + counts.get("New Member", 0)
            last_seen_ids = len(last_presence.student_statuses) if last_presence else 0
            event_type = "Possible Roster Gap / Returned" if returned_term else "Possible Roster Disappearance"
            evidence_summary = (
                f"{chapter} was present in {last_seen_before_gap}, missing from "
                f"{missing_terms[0]} through {missing_terms[-1]}"
                + (f", and returned in {returned_term}." if returned_term else ".")
            )
            key_text = f"{chapter_sort_key(chapter)}|{missing_terms[0]}|{missing_terms[-1]}|{returned_term}"
            rows.append(
                {
                    "candidate_key": hashlib.sha1(key_text.encode("utf-8")).hexdigest()[:16],
                    "chapter": chapter,
                    "candidate_event_type": event_type,
                    "suggested_event_type": "Chapter Kicked",
                    "confidence": "Needs Review",
                    "review_status": "Needs Review",
                    "first_seen_term": first_seen,
                    "last_seen_term": last_seen,
                    "last_seen_before_gap": last_seen_before_gap,
                    "missing_start_term": missing_terms[0],
                    "missing_end_term": missing_terms[-1],
                    "returned_term": returned_term,
                    "terms_missing_count": len(missing_terms),
                    "missing_terms": " | ".join(missing_terms),
                    "terms_present_count": terms_present_count,
                    "total_unique_valid_banner_ids_seen": len(all_student_ids),
                    "last_seen_unique_valid_banner_ids": last_seen_ids,
                    "last_seen_active_or_new_count": active_or_new,
                    "source_files_last_seen": source_files,
                    "evidence_source": "Roster coverage gap candidate",
                    "evidence_file_or_url": "",
                    "evidence_summary": evidence_summary,
                    "notes": "",
                }
            )

    return rows


def build_chapter_semester_tables(rows: Iterable[Mapping[str, object]]) -> ChapterSemesterTables:
    presence_by_key: dict[tuple[str, str], ChapterTermPresence] = {}
    source_rows = 0
    valid_rows = 0
    invalid_id_rows = 0
    excluded_position_rows = 0

    for row in rows:
        source_rows += 1
        if is_excluded_position(row.get("org_position_raw", "")):
            excluded_position_rows += 1
            continue

        student_id = normalize_banner_id(row.get("student_id", "") or row.get("student_id_raw", ""))
        if not student_id:
            invalid_id_rows += 1
            continue

        chapter = clean_text(row.get("chapter", "")) or clean_text(row.get("chapter_raw", ""))
        if not chapter:
            continue

        term_code, term_label, term_sort = term_from_row(row)
        if not term_code and not term_label:
            continue

        valid_rows += 1
        term_key = term_code or term_label
        key = (term_key, chapter)
        if key not in presence_by_key:
            presence_by_key[key] = ChapterTermPresence(
                term_code=term_code,
                term_label=term_label,
                term_sort=term_sort,
                chapter=chapter,
            )
        presence_by_key[key].add(
            student_id=student_id,
            status=normalize_status(row.get("org_status_bucket", ""), row.get("org_status_raw", "")),
            source_file=source_file_name(row.get("source_file", "")),
        )

    terms = sorted_term_keys(presence_by_key)
    inventory = inventory_rows(presence_by_key)
    matrix_columns, matrix = matrix_rows(presence_by_key, terms)
    lifecycle = lifecycle_rows(presence_by_key, terms)
    candidates = chapter_status_event_candidate_rows(presence_by_key, terms)
    return ChapterSemesterTables(
        inventory_rows=inventory,
        matrix_columns=matrix_columns,
        matrix_rows=matrix,
        lifecycle_rows=lifecycle,
        status_event_candidate_rows=candidates,
        term_count=len(terms),
        chapter_count=len(matrix),
        source_rows=source_rows,
        valid_rows=valid_rows,
        invalid_id_rows=invalid_id_rows,
        excluded_position_rows=excluded_position_rows,
    )


def write_csv(path: Path, columns: list[str], rows: Iterable[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def build_chapter_semester_exports(roster_path: Path, output_dir: Path) -> ChapterSemesterExportResult:
    with roster_path.open("r", newline="", encoding="utf-8-sig") as handle:
        tables = build_chapter_semester_tables(csv.DictReader(handle))

    inventory_path = output_dir / "chapter_semester_inventory.csv"
    matrix_path = output_dir / "chapter_semester_matrix.csv"
    lifecycle_path = output_dir / "chapter_lifecycle_review_template.csv"
    candidates_path = output_dir / "chapter_status_event_candidates.csv"
    write_csv(inventory_path, INVENTORY_COLUMNS, tables.inventory_rows)
    write_csv(matrix_path, tables.matrix_columns, tables.matrix_rows)
    write_csv(lifecycle_path, LIFECYCLE_COLUMNS, tables.lifecycle_rows)
    write_csv(candidates_path, CHAPTER_STATUS_EVENT_CANDIDATE_COLUMNS, tables.status_event_candidate_rows)

    return ChapterSemesterExportResult(
        inventory_path=inventory_path,
        matrix_path=matrix_path,
        lifecycle_review_path=lifecycle_path,
        status_event_candidates_path=candidates_path,
        inventory_rows=len(tables.inventory_rows),
        matrix_rows=len(tables.matrix_rows),
        lifecycle_rows=len(tables.lifecycle_rows),
        status_event_candidate_rows=len(tables.status_event_candidate_rows),
        term_count=tables.term_count,
        chapter_count=tables.chapter_count,
        source_rows=tables.source_rows,
        valid_rows=tables.valid_rows,
        invalid_id_rows=tables.invalid_id_rows,
        excluded_position_rows=tables.excluded_position_rows,
    )
