from pathlib import Path

from src.path_config import load_path_config, validate_path_config


def test_load_path_config_expands_root_and_creates_output_dirs(tmp_path: Path) -> None:
    raw_root = tmp_path / "shared raw"
    rosters = raw_root / "Rosters"
    grades = raw_root / "Grade Reports"
    rosters.mkdir(parents=True)
    grades.mkdir(parents=True)
    config = tmp_path / "paths.yaml"
    config.write_text(
        "\n".join(
            [
                f'raw_data_root: "{raw_root}"',
                'rosters_root: "${raw_data_root}/Rosters"',
                'roster_inbox_root: "${raw_data_root}/Rosters"',
                'grade_reports_root: "${raw_data_root}/Grade Reports"',
                f'output_root: "{tmp_path / "out"}"',
                f'cache_root: "{tmp_path / "cache"}"',
            ]
        ),
        encoding="utf-8",
    )

    paths = load_path_config(config)

    assert paths.raw_data_root == raw_root.resolve()
    assert paths.rosters_root == rosters.resolve()
    assert paths.grade_reports_root == grades.resolve()
    assert paths.output_root.exists()
    assert paths.cache_root.exists()
    assert not validate_path_config(paths, required_source_keys=["raw_data_root", "rosters_root", "grade_reports_root"])
