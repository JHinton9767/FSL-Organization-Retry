from pathlib import Path

import pandas as pd

from scripts.build_banner_id_batches import (
    DEFAULT_BATCH_SIZE,
    build_banner_id_batches,
    is_banner_id_header,
    normalize_candidate_banner_id,
    split_batches,
    validate_banner_id,
)


def test_valid_banner_id_recognition_and_lowercase_normalization() -> None:
    valid, reason, normalized = validate_banner_id(" a01234567 ")

    assert valid == "A01234567"
    assert reason is None
    assert normalized == "A01234567"


def test_invalid_banner_ids_are_rejected() -> None:
    invalid_values = ["A0123456", "A012345678", "123456789", "A0ABC1234", "Active"]

    for value in invalid_values:
        valid, reason, _ = validate_banner_id(value)
        assert valid is None
        assert reason in {"wrong_length", "wrong_prefix", "non_numeric_suffix", "not_banner_id_format"}


def test_blank_and_null_handling() -> None:
    assert validate_banner_id("")[1] == "blank_or_null"
    assert validate_banner_id(None)[1] == "blank_or_null"


def test_header_normalization_accepts_mixed_banner_headers() -> None:
    assert is_banner_id_header("Banner ID")
    assert is_banner_id_header("Banner_ID")
    assert is_banner_id_header("Texas State ID")
    assert is_banner_id_header("Unique-ID")
    assert not is_banner_id_header("Student Status")


def test_split_batches_handles_999_and_1000_ids() -> None:
    ids_999 = [f"A0{index:07d}" for index in range(999)]
    ids_1000 = [f"A0{index:07d}" for index in range(1000)]

    assert len(split_batches(ids_999, DEFAULT_BATCH_SIZE)) == 1
    assert [len(batch) for batch in split_batches(ids_1000, DEFAULT_BATCH_SIZE)] == [999, 1]


def test_build_batches_deduplicates_sorts_and_writes_rejected_audit(tmp_path: Path) -> None:
    source = tmp_path / "source"
    source.mkdir()
    pd.DataFrame(
        {
            "Student ID": ["a00000002", "A00000001", "A00000002", "A0ABC1234", "Active", ""],
            "Status": ["Active", "New", "Active", "Bad ID", "Header-like", ""],
        }
    ).to_csv(source / "roster.csv", index=False)
    output = tmp_path / "out"

    _, summary = build_banner_id_batches(
        output_dir=output,
        batch_size=999,
        include_raw=False,
        include_canonical=False,
        dry_run=False,
        verbose=False,
    )
    assert summary["total_valid_unique_banner_ids"] == 0

    from scripts import build_banner_id_batches as builder

    original_build_source_roots = builder.build_source_roots
    try:
        builder.build_source_roots = lambda include_raw, include_canonical, config_path=None: [source]
        _, summary = builder.build_banner_id_batches(output_dir=output)
    finally:
        builder.build_source_roots = original_build_source_roots

    master = pd.read_csv(output / "banner_ids_master.csv")
    rejected = pd.read_csv(output / "rejected_banner_id_values.csv")
    batch = pd.read_csv(output / "banner_ids_batch_001.csv")
    batch_txt = (output / "banner_ids_batch_001.txt").read_text(encoding="utf-8").splitlines()

    assert summary["total_valid_unique_banner_ids"] == 2
    assert master["Banner ID"].tolist() == ["A00000001", "A00000002"]
    assert batch["Banner ID"].tolist() == ["A00000001", "A00000002"]
    assert batch_txt == ["A00000001", "A00000002"]
    assert "non_numeric_suffix" in set(rejected["rejection_reason"])


def test_mixed_spreadsheet_headers_are_read(tmp_path: Path) -> None:
    source = tmp_path / "source"
    source.mkdir()
    workbook = source / "mixed.xlsx"
    pd.DataFrame(
        [
            ["Title row", "", ""],
            ["Last Name", "Unique ID", "Status"],
            ["Doe", "a00000003", "Active"],
            ["Smith", "New", "New Member"],
        ]
    ).to_excel(workbook, index=False, header=False)
    output = tmp_path / "out"

    from scripts import build_banner_id_batches as builder

    original_build_source_roots = builder.build_source_roots
    try:
        builder.build_source_roots = lambda include_raw, include_canonical, config_path=None: [source]
        _, summary = builder.build_banner_id_batches(output_dir=output)
    finally:
        builder.build_source_roots = original_build_source_roots

    master = pd.read_csv(output / "banner_ids_master.csv")
    assert summary["total_valid_unique_banner_ids"] == 1
    assert master["Banner ID"].tolist() == ["A00000003"]
