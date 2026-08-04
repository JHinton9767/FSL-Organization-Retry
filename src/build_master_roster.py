from __future__ import annotations

import re
from collections import defaultdict
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

from src.shared_utils import clean_text


ROOT = Path(__file__).resolve().parent.parent
DEFAULT_INPUT_ROOT = ROOT / "data" / "inbox" / "rosters"
SUPPORTED_EXTENSIONS = {".xlsx", ".xlsm", ".xltx", ".xltm"}
ROSTER_SOURCE_EXTENSIONS = SUPPORTED_EXTENSIONS.union({".pdf"})
SEMESTER_FOLDER_RE = re.compile(r"^(Fall|Spring)\s+(20\d{2})$", re.IGNORECASE)

HEADER_ALIASES = {
    "last_name": [
        "last name",
        "lastname",
        "surname",
        "member last name",
    ],
    "first_name": [
        "first name",
        "firstname",
        "given name",
        "member first name",
    ],
    "banner_id": [
        "banner id",
        "student id",
        "banner",
        "student number",
        "banner number",
        "z number",
    ],
    "email": [
        "email",
        "e-mail",
        "email address",
        "student email",
    ],
    "status": [
        "status",
        "member status",
        "membership status",
        "roster status",
    ],
    "semester_joined": [
        "semester joined",
        "joined",
        "join term",
        "semester initiated",
        "term joined",
        "semester admitted",
        "initiation term",
    ],
    "position": [
        "position",
        "office",
        "role",
        "member/council",
        "member council",
        "title",
    ],
    "chapter": [
        "chapter",
        "org name",
        "organization",
        "org",
        "group",
        "fraternity/sorority",
        "fsl organization",
    ],
}

CANONICAL_ALIAS_MAP = {
    standard_name: {re.sub(r"\s+", " ", re.sub(r"[^a-z0-9 ]+", "", alias.lower().replace("_", " "))).strip() for alias in aliases}
    for standard_name, aliases in HEADER_ALIASES.items()
}

STATUS_MAP = {
    "A": "Active",
    "AL": "Alumni",
    "G": "Graduated",
    "H": "H",
    "I": "Inactive",
    "S": "Suspended",
    "N": "New Member",
    "RS": "Resigned",
    "RV": "Revoked",
    "T": "Transfer",
}

MONTH_PATTERNS = [
    (1, r"\bjan(?:uary)?\b"),
    (2, r"\bfeb(?:ruary)?\b"),
    (3, r"\bmar(?:ch)?\b"),
    (4, r"\bapr(?:il)?\b"),
    (5, r"\bmay\b"),
    (6, r"\bjun(?:e)?\b"),
    (7, r"\bjul(?:y)?\b"),
    (8, r"\baug(?:ust)?\b"),
    (9, r"\bsep(?:t|tember)?\b"),
    (10, r"\boct(?:ober)?\b"),
    (11, r"\bnov(?:ember)?\b"),
    (12, r"\bdec(?:ember)?\b"),
]


def roster_file_version_priority(source_file: str) -> float:
    text = clean_text(source_file).lower()
    if re.search(r"\bfinal\b", text):
        return 3
    has_revised = bool(re.search(r"\brevised\b|\brevision\b|\brev\b", text))
    has_updated = bool(re.search(r"\bupdated\b|\bupdate\b", text))
    if has_revised and has_updated:
        return 2.5
    if has_revised or has_updated:
        return 2
    return 1


def roster_file_month_priority(source_file: str) -> int:
    text = re.sub(r"[_\-.]+", " ", clean_text(source_file).lower())
    for month_number, pattern in MONTH_PATTERNS:
        if re.search(pattern, text):
            return month_number
    return 0


def source_file_format_priority(source_file: str) -> int:
    suffix = Path(clean_text(source_file)).suffix.lower()
    if suffix == ".pdf":
        return 1
    if suffix == ".csv":
        return 2
    if suffix in {".xlsx", ".xls", ".xlsm", ".xlsb"}:
        return 3
    return 0

GREEK_LETTER_WORDS = {
    "alpha",
    "beta",
    "gamma",
    "delta",
    "epsilon",
    "zeta",
    "eta",
    "theta",
    "iota",
    "kappa",
    "lambda",
    "mu",
    "nu",
    "xi",
    "omicron",
    "pi",
    "rho",
    "sigma",
    "tau",
    "upsilon",
    "phi",
    "chi",
    "psi",
    "omega",
}
ALLOWED_CHAPTER_PHRASES = {
    "order of omega": "Order of Omega",
    "kappa alpha order": "Kappa Alpha Order",
}

CHAPTER_JUNK_PATTERNS = [
    r"never responded to email",
    r"greek leadership honor society",
    r"fraternity,\s*inc\.?",
    r"fraternity",
    r"sorority",
    r"roster revised 2",
    r"roster[_\s-]*update",
    r"revised",
    r"updated",
    r"update",
    r"final",
    r"roster",
    r"sept",
    r"nov",
    r"\bfall\s*20\d{2}\b",
    r"\b(19|20)\d{2}\b",
    r"\b2\b",
]

GENERIC_ROSTER_CONTEXT_PATTERNS = [
    r"^(copy of )?rosters?$",
    r"^raw rosters?$",
    r"^raw data$",
    r"^master roster$",
    r"^(ifc|phc|nphc|mcg)$",
    r"^(ifc|phc|nphc|mcg)\s+rosters?$",
    r"^(ifc|phc|nphc|mcg)\s+council$",
    r"^all greek(?: life)?$",
    r"^greek life$",
    r"^entire council$",
    r"^all fraternit(?:y|ies)$",
    r"^all sororit(?:y|ies)$",
    r"^(initial|final|revised|revision|updated|update)(?:\s+rosters?)?$",
]

NEW_MEMBER_CONTEXT_PATTERNS = [
    r"\bnew\s*members?\b",
    r"\bassociate\s*members?\b",
]

INDIVIDUAL_FORM_CONTEXT_PATTERNS = [
    r"\binput\s*forms?\b",
    r"\bforms?\b",
    r"\bsigned\b",
    r"\bsignature\b",
    r"\bapplication\b",
    r"\bpaperwork\b",
    r"\bpacket\b",
]

PERSON_NAME_NOISE_PATTERNS = [
    r"\bnew\s*members?\b",
    r"\bassociate\s*members?\b",
    r"\binput\s*forms?\b",
    r"\bforms?\b",
    r"\bsigned\b",
    r"\bsignature\b",
    r"\bapplication\b",
    r"\bpaperwork\b",
    r"\bpacket\b",
    r"\bcopy of rosters?\b",
    r"\brosters?\b",
    r"\braw data\b",
    r"\braw rosters?\b",
    r"\bmaster roster\b",
    r"\bcouncil\b",
    r"\bgreek life\b",
    r"\ball greek\b",
    r"\bfall\s+20\d{2}\b",
    r"\bspring\s+20\d{2}\b",
    r"\bifc\b",
    r"\bphc\b",
    r"\bnphc\b",
    r"\bmcg\b",
    r"\bfinal\b",
    r"\binitial\b",
    r"\brevised\b",
    r"\bupdated\b",
]


def source_file_label(path: Path, root: Optional[Path] = None) -> str:
    if root is not None:
        try:
            return str(path.resolve().relative_to(root.resolve()))
        except ValueError:
            pass
    try:
        return str(path.resolve().relative_to(DEFAULT_INPUT_ROOT.resolve()))
    except ValueError:
        return path.name


def canonical_header(value: object) -> str:
    text = clean_text(value).lower()
    text = text.replace("_", " ")
    text = re.sub(r"[^a-z0-9 ]+", "", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def header_matches(standard_name: str, header: str) -> bool:
    aliases = CANONICAL_ALIAS_MAP[standard_name]
    if header in aliases:
        return True

    if standard_name == "status":
        return header.startswith("status") or " status " in f" {header} "

    return any(alias in header for alias in aliases if len(alias) > 4)


def normalize_status(value: str) -> str:
    raw = clean_text(value)
    upper = raw.upper()
    if upper in STATUS_MAP:
        return STATUS_MAP[upper]
    return raw


def is_excluded_roster_position(value: object) -> bool:
    text = clean_text(value).lower()
    if not text:
        return False
    return bool(re.search(r"\b(?:advisor|adviser)\b|\bgreek\s+staff\b", text))


VALID_BANNER_ID_RE = re.compile(r"^A0\d{7}$")


def normalize_banner_id(value: str) -> str:
    text = clean_text(value)
    if not text:
        return ""
    text = re.sub(r"\.0$", "", text, flags=re.IGNORECASE)
    text = text.upper()
    return text if VALID_BANNER_ID_RE.fullmatch(text) else ""


def detect_inline_chapter_label(row: Tuple[object, ...], header_map: Dict[str, int]) -> str:
    protected_fields = ["last_name", "first_name", "banner_id", "email", "status", "semester_joined", "position"]
    if any(get_cell(row, header_map.get(field)) for field in protected_fields):
        return ""

    non_empty = [clean_text(value) for value in row if clean_text(value)]
    if not non_empty or len(non_empty) > 2:
        return ""

    for value in non_empty:
        normalized = normalize_chapter_name(value)
        if normalized and normalized != "Unknown" and not is_excluded_chapter(normalized):
            return value
    return ""


def parse_term_from_path(path: Path) -> Tuple[str, str]:
    for part in path.parts:
        match = SEMESTER_FOLDER_RE.fullmatch(part)
        if match:
            return match.group(2), f"{match.group(1).title()} {match.group(2)}"

    for candidate in [path.parent.name, path.stem]:
        match = SEMESTER_FOLDER_RE.search(candidate)
        if match:
            return match.group(2), f"{match.group(1).title()} {match.group(2)}"

    year_match = re.search(r"(20\d{2}|19\d{2})", path.stem)
    if year_match:
        return year_match.group(1), year_match.group(1)
    return "Unknown", "Unknown"


def is_placeholder_sheet_name(value: str) -> bool:
    normalized = re.sub(r"[\s_]+", "", clean_text(value)).lower()
    return normalized in {"sheet1", "sheet2", "sheet3"}


def normalize_chapter_name(value: str) -> str:
    cleaned = clean_text(value)
    if not cleaned:
        return ""

    for pattern in CHAPTER_JUNK_PATTERNS:
        cleaned = re.sub(pattern, " ", cleaned, flags=re.IGNORECASE)

    cleaned = re.sub(r"[_.,]+", " ", cleaned)
    cleaned = re.sub(r"[^A-Za-z()\-\s]+", " ", cleaned)

    lowered_cleaned = re.sub(r"\s+", " ", cleaned).strip().lower()
    for phrase, canonical in ALLOWED_CHAPTER_PHRASES.items():
        if phrase in lowered_cleaned:
            return canonical

    parts = re.findall(r"[A-Za-z]+|[()-]", cleaned)
    kept_parts: List[str] = []
    for part in parts:
        lower = part.lower()
        if part in {"(", ")", "-"}:
            kept_parts.append(part)
        elif lower in GREEK_LETTER_WORDS:
            kept_parts.append(lower.title())

    normalized = " ".join(kept_parts)
    normalized = re.sub(r"\s*-\s*", "-", normalized)
    normalized = re.sub(r"\(\s+", "(", normalized)
    normalized = re.sub(r"\s+\)", ")", normalized)
    normalized = re.sub(r"\(\)", "", normalized)
    normalized = normalized.replace("Alpha Kappa Alpha (Sigma Epsilon)", "Alpha Kappa Alpha")
    normalized = normalized.replace("Phi Kappa Tau-Gamma Psi", "Phi Kappa Tau")
    normalized = normalized.replace("Sigma Iota Alpha (Sigma Iota Alpha)", "Sigma Iota Alpha")
    normalized = re.sub(r"\s+", " ", normalized).strip(" -")
    return normalized or "Unknown"


def is_order_of_omega(chapter: str) -> bool:
    return normalize_chapter_name(chapter) == "Order of Omega"


def is_excluded_chapter(chapter: str) -> bool:
    normalized = normalize_chapter_name(chapter)
    return normalized in {"Order of Omega", "Epsilon Lambda Alpha"}


def is_generic_roster_context_name(value: str) -> bool:
    text = re.sub(r"[_\-.]+", " ", clean_text(value)).strip().lower()
    if not text:
        return True
    if SEMESTER_FOLDER_RE.fullmatch(text.title()):
        return True
    if re.fullmatch(r"(19|20)\d{2}", text):
        return True
    return any(re.fullmatch(pattern, text, flags=re.IGNORECASE) for pattern in GENERIC_ROSTER_CONTEXT_PATTERNS)


def iter_source_context_candidates(path: Path, sheet_name: str = "") -> List[str]:
    candidates: List[str] = []
    if sheet_name:
        candidates.append(sheet_name)
    candidates.extend([path.stem, path.name])
    for part in path.parts:
        candidates.append(part)

    unique_candidates: List[str] = []
    seen: set[str] = set()
    for candidate in candidates:
        key = clean_text(candidate).lower()
        if not key or key in seen:
            continue
        seen.add(key)
        unique_candidates.append(candidate)
    return unique_candidates


def source_context_indicates_new_member(path: Path, sheet_name: str = "") -> bool:
    return any(
        re.search(pattern, clean_text(candidate), flags=re.IGNORECASE)
        for candidate in iter_source_context_candidates(path, sheet_name)
        for pattern in NEW_MEMBER_CONTEXT_PATTERNS
    )


def source_context_indicates_individual_form(path: Path) -> bool:
    return any(
        re.search(pattern, clean_text(candidate), flags=re.IGNORECASE)
        for candidate in iter_source_context_candidates(path)
        for pattern in INDIVIDUAL_FORM_CONTEXT_PATTERNS
    )


def extract_person_name_from_label(value: str) -> Optional[Tuple[str, str]]:
    cleaned = clean_text(value)
    if not cleaned:
        return None

    for pattern in PERSON_NAME_NOISE_PATTERNS:
        cleaned = re.sub(pattern, " ", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"[_\-.]+", " ", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip(" ,-_")
    if not cleaned:
        return None
    if normalize_chapter_name(cleaned) not in {"", "Unknown"}:
        return None

    if "," in cleaned:
        left, right = [part.strip() for part in cleaned.split(",", 1)]
        left_tokens = re.findall(r"[A-Za-z]+", left)
        right_tokens = re.findall(r"[A-Za-z]+", right)
        if left_tokens and right_tokens:
            first_name = right_tokens[0].title()
            last_name = left_tokens[-1].title()
            if first_name and last_name:
                return first_name, last_name

    tokens = [token.title() for token in re.findall(r"[A-Za-z]+", cleaned)]
    if len(tokens) < 2 or len(tokens) > 5:
        return None
    if all(token.lower() in GREEK_LETTER_WORDS for token in tokens):
        return None

    return tokens[0], tokens[-1]


def is_individual_new_member_form_pdf(path: Path) -> bool:
    if path.suffix.lower() != ".pdf":
        return False
    return extract_person_name_from_label(path.stem) is not None and (
        source_context_indicates_new_member(path)
        or source_context_indicates_individual_form(path)
        or not any(re.search(pattern, clean_text(path.stem), flags=re.IGNORECASE) for pattern in GENERIC_ROSTER_CONTEXT_PATTERNS)
    )


def build_individual_new_member_form_lookup(paths: Iterable[Path], root: Optional[Path] = None) -> Dict[Tuple[str, str, str, str], List[str]]:
    lookup: Dict[Tuple[str, str, str, str], List[str]] = defaultdict(list)
    for path in paths:
        if not is_individual_new_member_form_pdf(path):
            continue
        person = extract_person_name_from_label(path.stem)
        if not person:
            continue
        academic_year, term = parse_term_from_path(path)
        if term == "Unknown":
            continue
        first_name, last_name = person
        lookup[(academic_year.lower(), term.lower(), first_name.lower(), last_name.lower())].append(source_file_label(path, root))
    return dict(lookup)


def should_upgrade_to_new_member_status(status: str, position: str, source_is_new_member: bool, has_form_evidence: bool) -> bool:
    normalized_status = clean_text(normalize_status(status))
    position_text = clean_text(position).lower()
    explicit_position = "new member" in position_text
    if normalized_status == "New Member":
        return True
    if normalized_status not in {"", "Active"}:
        return False
    return explicit_position or source_is_new_member or has_form_evidence


def iter_chapter_context_candidates(path: Path, sheet_name: str) -> List[str]:
    candidates: List[str] = []
    if sheet_name and not is_placeholder_sheet_name(sheet_name):
        candidates.append(sheet_name)
    candidates.append(path.stem)
    for parent in list(path.parents)[:4]:
        if parent.name:
            candidates.append(parent.name)

    unique_candidates: List[str] = []
    seen: set[str] = set()
    for candidate in candidates:
        key = clean_text(candidate).lower()
        if not key or key in seen:
            continue
        seen.add(key)
        unique_candidates.append(candidate)
    return unique_candidates


def chapter_from_filename(path: Path) -> str:
    stem = clean_text(path.stem)
    if not stem:
        return ""

    if stem.lower() == "raw roster data" or is_generic_roster_context_name(stem):
        return "Unknown"

    return normalize_chapter_name(stem)


def infer_chapter(path: Path, sheet_name: str) -> str:
    for candidate in iter_chapter_context_candidates(path, sheet_name):
        if is_generic_roster_context_name(candidate):
            continue
        cleaned = normalize_chapter_name(candidate)
        if not cleaned:
            continue
        if SEMESTER_FOLDER_RE.fullmatch(cleaned):
            continue
        if cleaned.lower() in {"copy of rosters", "rosters", "raw rosters", "master roster", "unknown"}:
            continue
        if re.fullmatch(r"(19|20)\d{2}", cleaned):
            continue
        return cleaned
    return ""


def score_header_row(values: List[object]) -> Tuple[int, Dict[str, int]]:
    matched: Dict[str, int] = {}
    canon = [canonical_header(value) for value in values]
    for idx, header in enumerate(canon):
        for standard_name in CANONICAL_ALIAS_MAP:
            if header_matches(standard_name, header) and standard_name not in matched:
                matched[standard_name] = idx
    return len(matched), matched


def find_status_column(ws, max_scan_rows: int = 30) -> Tuple[Optional[int], Optional[int]]:
    best_match: Tuple[int, Optional[int], Optional[int]] = (0, None, None)

    for row_idx, row in enumerate(ws.iter_rows(min_row=1, max_row=min(ws.max_row, max_scan_rows), values_only=True), start=1):
        for col_idx, value in enumerate(row):
            header = canonical_header(value)
            if not header:
                continue

            score = 0
            if header == "status":
                score = 3
            elif header.startswith("status"):
                score = 2
            elif " status " in f" {header} ":
                score = 1

            if score > best_match[0]:
                best_match = (score, row_idx, col_idx)

    return best_match[1], best_match[2]


def find_status_column_in_rows(rows: List[Tuple[object, ...]], max_scan_rows: int = 30) -> Tuple[Optional[int], Optional[int]]:
    best_match: Tuple[int, Optional[int], Optional[int]] = (0, None, None)

    for row_idx, row in enumerate(rows[:max_scan_rows], start=1):
        for col_idx, value in enumerate(row):
            header = canonical_header(value)
            if not header:
                continue

            score = 0
            if header == "status":
                score = 3
            elif header.startswith("status"):
                score = 2
            elif " status " in f" {header} ":
                score = 1

            if score > best_match[0]:
                best_match = (score, row_idx, col_idx)

    return best_match[1], best_match[2]


def find_header_row(ws) -> Tuple[Optional[int], Dict[str, int]]:
    best_score = 0
    best_row_idx = None
    best_map: Dict[str, int] = {}

    for row_idx, row in enumerate(ws.iter_rows(min_row=1, max_row=min(ws.max_row, 25), values_only=True), start=1):
        score, header_map = score_header_row(list(row))
        if score > best_score:
            best_score = score
            best_row_idx = row_idx
            best_map = header_map

    required = {"last_name", "first_name"}
    if best_row_idx is None or best_score < 3 or not required.issubset(best_map):
        return None, {}
    return best_row_idx, best_map


def find_header_row_in_rows(rows: List[Tuple[object, ...]]) -> Tuple[Optional[int], Dict[str, int]]:
    best_score = 0
    best_row_idx = None
    best_map: Dict[str, int] = {}

    for row_idx, row in enumerate(rows[:25], start=1):
        score, header_map = score_header_row(list(row))
        if score > best_score:
            best_score = score
            best_row_idx = row_idx
            best_map = header_map

    required = {"last_name", "first_name"}
    if best_row_idx is None or best_score < 3 or not required.issubset(best_map):
        return None, {}
    return best_row_idx, best_map


def get_cell(row: Tuple[object, ...], index: Optional[int]) -> str:
    if index is None or index >= len(row):
        return ""
    return clean_text(row[index])


def row_is_empty(values: Iterable[str]) -> bool:
    return all(not clean_text(value) for value in values)


def pdf_table_rows(path: Path) -> Tuple[List[Tuple[str, List[Tuple[object, ...]]]], List[str]]:
    try:
        import pdfplumber
    except ImportError:
        return [], [f"PDF skipped because pdfplumber is not installed. Run py -m pip install -r requirements.txt. File: {path}"]

    table_sources: List[Tuple[str, List[Tuple[object, ...]]]] = []
    issues: List[str] = []
    try:
        with pdfplumber.open(path) as pdf:
            for page_idx, page in enumerate(pdf.pages, start=1):
                tables = page.extract_tables() or []
                for table_idx, table in enumerate(tables, start=1):
                    rows = [
                        tuple("" if cell is None else cell for cell in row)
                        for row in table
                        if row and any(clean_text(cell) for cell in row)
                    ]
                    if rows:
                        table_sources.append((f"Page {page_idx} Table {table_idx}", rows))
                if tables:
                    continue
                text = page.extract_text() or ""
                text_rows = [
                    tuple(part for part in re.split(r"\s{2,}|\t+", line.strip()) if part)
                    for line in text.splitlines()
                    if line.strip()
                ]
                if text_rows:
                    table_sources.append((f"Page {page_idx} Text", text_rows))
    except Exception as exc:
        issues.append(f"FAILED to open PDF {path}: {exc}")

    if not table_sources and not issues:
        issues.append(f"PDF skipped because no extractable table/text rows were found in {path}.")
    return table_sources, issues
