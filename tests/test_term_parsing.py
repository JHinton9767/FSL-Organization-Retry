from app.io_utils import parse_term_label


def test_parse_term_variants() -> None:
    assert parse_term_label("Fall 2021")["code"] == "2021FA"
    assert parse_term_label("2021FA")["year"] == 2021
    assert parse_term_label("2021-Fall")["season"] == "Fall"

