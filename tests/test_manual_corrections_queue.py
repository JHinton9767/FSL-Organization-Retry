import pandas as pd

from app.main import _build_manual_assignment_queue, _canonical_review_queue_for_app


class _Bundle:
    def __init__(self, *, summary: pd.DataFrame, manual_review_queue: pd.DataFrame) -> None:
        self.summary = summary
        self.tables = {"manual_review_queue": manual_review_queue}


def test_manual_assignment_queue_uses_true_unknown_outcomes_only() -> None:
    summary = pd.DataFrame(
        {
            "student_id": ["A00000001", "A00000002", "A00000003", "bad-id"],
            "student_name": ["Unknown One", "Kicked One", "Dropped One", "Bad Id"],
            "chapter": ["Alpha", "Beta", "Gamma", "Delta"],
            "join_term": ["Fall 2020", "Fall 2020", "Fall 2020", "Fall 2020"],
            "latest_outcome_bucket": [
                "Unknown / Manual Review Required",
                "Chapter Kicked",
                "Dropped/Resigned",
                "Unknown / Manual Review Required",
            ],
            "outcome_resolution_group": [
                "Unknown / Manual Review Required",
                "Chapter Kicked",
                "Dropped/Resigned",
                "Unknown / Manual Review Required",
            ],
        }
    )

    queue = _build_manual_assignment_queue(summary, set())

    assert queue["student_id"].tolist() == ["A00000001"]
    assert queue["outcome_bucket"].tolist() == ["Unknown"]


def test_canonical_review_queue_excludes_resolved_chapter_kicked_soft_issues() -> None:
    summary = pd.DataFrame(
        {
            "student_id": ["A00000001", "A00000002", "A00000003"],
            "student_name": ["Unknown One", "Kicked One", "Conflict One"],
            "chapter": ["Alpha", "Beta", "Gamma"],
            "join_term": ["Fall 2020", "Fall 2020", "Fall 2020"],
            "latest_outcome_bucket": [
                "Unknown / Manual Review Required",
                "Chapter Kicked",
                "Chapter Kicked",
            ],
        }
    )
    canonical = pd.DataFrame(
        {
            "review_id": ["unknown-soft", "kicked-soft", "kicked-hard"],
            "student_id": ["A00000001", "A00000002", "A00000003"],
            "normalized_student_id": ["A00000001", "A00000002", "A00000003"],
            "current_outcome_bucket": [
                "Unknown / Manual Review Required",
                "Chapter Kicked",
                "Chapter Kicked",
            ],
            "issue_type": [
                "roster_without_grade_report",
                "roster_without_grade_report",
                "multiple_chapters_same_term",
            ],
            "issue_description": ["soft unknown", "soft kicked", "hard kicked"],
            "priority": ["Medium", "Medium", "High"],
        }
    )

    queue = _canonical_review_queue_for_app(_Bundle(summary=summary, manual_review_queue=canonical), set())

    assert queue["student_id"].tolist() == ["A00000001", "A00000003"]
    assert queue["issue_type"].tolist() == ["roster_without_grade_report", "multiple_chapters_same_term"]
