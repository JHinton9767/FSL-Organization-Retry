from pathlib import Path

import pandas as pd

from src.build_canonical_pipeline import build_transcript_text_cache_bundle


def test_transcript_text_parser_builds_term_and_course_rows(tmp_path: Path) -> None:
    transcript_dir = tmp_path / "transcript_text"
    transcript_dir.mkdir(parents=True, exist_ok=True)
    transcript_file = transcript_dir / "A01234567_Doe_Jane.txt"
    transcript_file.write_text(
        "\n".join(
            [
                "Spring 2024",
                "3 MC3117|LEC FREELANCING FOR MEDIA PROF. A",
                "3 MC3375|LEC ELEC. MEDIA AS ENTERTAINMENT B",
                "Term at a glance:",
                "Credits:",
                "6",
                "Credit Comp %:",
                "100%",
                "Term GPA:",
                "3.50",
                "Cum GPA:",
                "3.10",
                "Academic Standing:",
                "Good Standing",
                "Fall 2023",
                "0 (3) MATH1329|LEC MATH BUS & ECO II F",
                "3 MC4301|LEC MEDIA LAW & ETHICS C",
                "Term at a glance:",
                "Credits:",
                "3",
                "Credit Comp %:",
                "50%",
                "Term GPA:",
                "2.00",
                "Cum GPA:",
                "2.90",
                "Academic Standing:",
                "Good Standing",
            ]
        ),
        encoding="utf-8",
    )

    transcript_terms, transcript_courses, transcript_audit, transcript_issues, transcript_academic = build_transcript_text_cache_bundle(
        transcript_dir,
        pd.DataFrame(columns=["source_file", "student_id", "first_name", "last_name", "notes"]),
    )

    assert transcript_issues.empty
    assert len(transcript_terms) == 2
    assert len(transcript_courses) == 4
    assert len(transcript_academic) == 2
    assert transcript_audit.iloc[0]["parse_status"] == "parsed"
    assert transcript_academic["graduation_term_code"].fillna("").eq("").all()
    assert transcript_academic["academic_standing_raw"].tolist() == ["Good Standing", "Good Standing"]
    assert transcript_academic["student_id"].fillna("").astype(str).str.startswith("A").all()


def test_transcript_text_parser_ignores_pre_enrollment_section(tmp_path: Path) -> None:
    transcript_dir = tmp_path / "transcript_text"
    transcript_dir.mkdir(parents=True, exist_ok=True)
    transcript_file = transcript_dir / "A07654321_Smith_John.txt"
    transcript_file.write_text(
        "\n".join(
            [
                "Fall 2023",
                "3 ENG1310|LEC COLLEGE WRITING I A",
                "Term at a glance:",
                "Credits:",
                "3",
                "Credit Comp %:",
                "100%",
                "Term GPA:",
                "4.00",
                "Cum GPA:",
                "4.00",
                "Academic Standing:",
                "Good Standing",
                "Pre-Enrollment and Progression",
                "High School Cum GPA:",
                "90.1",
            ]
        ),
        encoding="utf-8",
    )

    transcript_terms, transcript_courses, transcript_audit, transcript_issues, transcript_academic = build_transcript_text_cache_bundle(
        transcript_dir,
        pd.DataFrame(columns=["source_file", "student_id", "first_name", "last_name", "notes"]),
    )

    assert transcript_issues.empty
    assert len(transcript_terms) == 1
    assert len(transcript_courses) == 1
    assert len(transcript_academic) == 1
    assert transcript_audit.iloc[0]["warning_count"] == 0
    assert transcript_audit.iloc[0]["unmatched_lines"] == ""
