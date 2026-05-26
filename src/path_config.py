from __future__ import annotations

import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional


ROOT = Path(__file__).resolve().parent.parent
DEFAULT_CONFIG_PATH = ROOT / "config" / "local_paths.yaml"
EXAMPLE_CONFIG_PATH = ROOT / "config" / "example_paths.yaml"
PATH_CONFIG_ENV = "FSL_PATH_CONFIG"


@dataclass(frozen=True)
class FSLPathConfig:
    config_path: Path
    used_example_config: bool
    raw_data_root: Path
    rosters_root: Path
    roster_inbox_root: Path
    grade_reports_root: Path
    transcript_text_root: Path
    graduation_root: Path
    snapshot_root: Path
    reference_root: Path
    membership_reference_root: Path
    gpa_reference_root: Path
    gpa_benchmark_root: Path
    output_root: Path
    cache_root: Path


def _strip_inline_comment(value: str) -> str:
    in_quote = False
    quote_char = ""
    for index, char in enumerate(value):
        if char in {'"', "'"}:
            if in_quote and char == quote_char:
                in_quote = False
                quote_char = ""
            elif not in_quote:
                in_quote = True
                quote_char = char
        elif char == "#" and not in_quote:
            return value[:index].strip()
    return value.strip()


def _read_flat_yaml(path: Path) -> Dict[str, str]:
    values: Dict[str, str] = {}
    for line_number, raw_line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if ":" not in line:
            raise ValueError(f"Invalid path config line {line_number} in {path}: {raw_line}")
        key, value = line.split(":", 1)
        key = key.strip()
        value = _strip_inline_comment(value)
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {'"', "'"}:
            value = value[1:-1]
        values[key] = value
    return values


def _expand_value(value: str, values: Dict[str, str]) -> str:
    expanded = value
    for _ in range(10):
        replaced = re.sub(r"\$\{([^}]+)\}", lambda match: values.get(match.group(1), match.group(0)), expanded)
        if replaced == expanded:
            break
        expanded = replaced
    return os.path.expandvars(expanded)


def _to_path(value: str, values: Dict[str, str]) -> Path:
    expanded = _expand_value(value, values).strip()
    path = Path(expanded).expanduser()
    if not path.is_absolute():
        path = ROOT / path
    return path.resolve()


def _resolve_user_path(value: str | Path) -> Path:
    path = Path(value).expanduser()
    if not path.is_absolute():
        path = ROOT / path
    return path.resolve()


def _git_bash_collapsed_config_path(value: str | Path) -> Optional[Path]:
    """Recover from Git Bash commands like --config config/local_paths.yaml.

    In Git Bash, an unquoted backslash can be consumed before Python sees the
    argument, so config/local_paths.yaml can arrive as configlocal_paths.yaml.
    """
    text = str(value).strip().replace("\\", "/")
    if "/" in text:
        return None
    lowered = text.lower()
    known = {
        "configlocal_paths.yaml": DEFAULT_CONFIG_PATH,
        "configexample_paths.yaml": EXAMPLE_CONFIG_PATH,
    }
    return known.get(lowered)


def _select_config_path(config_path: Optional[str | Path] = None) -> tuple[Path, bool]:
    if config_path:
        recovered = _git_bash_collapsed_config_path(config_path)
        if recovered and recovered.exists():
            return recovered.resolve(), recovered == EXAMPLE_CONFIG_PATH
        return _resolve_user_path(config_path), False

    env_value = os.getenv(PATH_CONFIG_ENV, "").strip()
    if env_value:
        recovered = _git_bash_collapsed_config_path(env_value)
        if recovered and recovered.exists():
            return recovered.resolve(), recovered == EXAMPLE_CONFIG_PATH
        return _resolve_user_path(env_value), False

    if DEFAULT_CONFIG_PATH.exists():
        return DEFAULT_CONFIG_PATH.resolve(), False
    return EXAMPLE_CONFIG_PATH.resolve(), True


def load_path_config(config_path: Optional[str | Path] = None) -> FSLPathConfig:
    selected_path, used_example = _select_config_path(config_path)
    if not selected_path.exists():
        raise FileNotFoundError(
            f"Path config not found: {selected_path}. Copy config/example_paths.yaml to "
            "config/local_paths.yaml and set raw_data_root to the shared-drive folder."
        )

    raw_values = _read_flat_yaml(selected_path)
    raw_values.setdefault("raw_data_root", "")
    raw_values.setdefault("output_root", "output/canonical")
    raw_values.setdefault("cache_root", "output/canonical/_source_cache")
    raw_values.setdefault("rosters_root", "${raw_data_root}/Rosters")
    raw_values.setdefault("roster_inbox_root", raw_values["rosters_root"])
    raw_values.setdefault("grade_reports_root", "${raw_data_root}/Grade Reports")
    raw_values.setdefault("transcript_text_root", "${raw_data_root}/Transcript Text")
    raw_values.setdefault("graduation_root", "${raw_data_root}/Graduation Files")
    raw_values.setdefault("snapshot_root", "${raw_data_root}/Current Snapshot")
    raw_values.setdefault("reference_root", "${raw_data_root}/Reference Files")
    raw_values.setdefault("membership_reference_root", "${raw_data_root}/Membership Reference")
    raw_values.setdefault("gpa_reference_root", "${raw_data_root}/GPA Reference")
    raw_values.setdefault("gpa_benchmark_root", "${raw_data_root}/GPA Benchmark Reference")

    output_root = _to_path(raw_values["output_root"], raw_values)
    cache_root = _to_path(raw_values["cache_root"], raw_values)
    output_root.mkdir(parents=True, exist_ok=True)
    cache_root.mkdir(parents=True, exist_ok=True)

    return FSLPathConfig(
        config_path=selected_path,
        used_example_config=used_example,
        raw_data_root=_to_path(raw_values["raw_data_root"], raw_values),
        rosters_root=_to_path(raw_values["rosters_root"], raw_values),
        roster_inbox_root=_to_path(raw_values["roster_inbox_root"], raw_values),
        grade_reports_root=_to_path(raw_values["grade_reports_root"], raw_values),
        transcript_text_root=_to_path(raw_values["transcript_text_root"], raw_values),
        graduation_root=_to_path(raw_values["graduation_root"], raw_values),
        snapshot_root=_to_path(raw_values["snapshot_root"], raw_values),
        reference_root=_to_path(raw_values["reference_root"], raw_values),
        membership_reference_root=_to_path(raw_values["membership_reference_root"], raw_values),
        gpa_reference_root=_to_path(raw_values["gpa_reference_root"], raw_values),
        gpa_benchmark_root=_to_path(raw_values["gpa_benchmark_root"], raw_values),
        output_root=output_root,
        cache_root=cache_root,
    )


def validate_path_config(paths: FSLPathConfig, required_source_keys: Optional[Iterable[str]] = None) -> List[str]:
    issues: List[str] = []
    source_paths = {
        "raw_data_root": paths.raw_data_root,
        "rosters_root": paths.rosters_root,
        "roster_inbox_root": paths.roster_inbox_root,
        "grade_reports_root": paths.grade_reports_root,
        "transcript_text_root": paths.transcript_text_root,
        "graduation_root": paths.graduation_root,
        "snapshot_root": paths.snapshot_root,
        "reference_root": paths.reference_root,
        "membership_reference_root": paths.membership_reference_root,
        "gpa_reference_root": paths.gpa_reference_root,
        "gpa_benchmark_root": paths.gpa_benchmark_root,
    }
    keys_to_check = list(required_source_keys or source_paths)
    for key in keys_to_check:
        path = source_paths[key]
        if not path.exists():
            level = "ERROR" if key == "raw_data_root" else "WARNING"
            issues.append(f"{level}: {key} does not exist: {path}")

    for key, path in {"output_root": paths.output_root, "cache_root": paths.cache_root}.items():
        try:
            path.mkdir(parents=True, exist_ok=True)
            probe = path / ".write_test"
            probe.write_text("ok", encoding="utf-8")
            probe.unlink(missing_ok=True)
        except OSError as exc:
            issues.append(f"ERROR: {key} is not writable: {path} ({exc})")
    return issues


def resolve_canonical_output_root() -> Path:
    try:
        return load_path_config().output_root
    except Exception:
        return ROOT / "output" / "canonical"
