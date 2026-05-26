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


def test_load_path_config_recovers_common_git_bash_collapsed_config_path(monkeypatch, tmp_path: Path) -> None:
    raw_root = tmp_path / "raw"
    raw_root.mkdir()
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    local_config = config_dir / "local_paths.yaml"
    local_config.write_text(
        "\n".join(
            [
                f'raw_data_root: "{raw_root}"',
                'rosters_root: "${raw_data_root}"',
                'grade_reports_root: "${raw_data_root}"',
                f'output_root: "{tmp_path / "out"}"',
                f'cache_root: "{tmp_path / "cache"}"',
            ]
        ),
        encoding="utf-8",
    )

    import src.path_config as path_config

    monkeypatch.setattr(path_config, "ROOT", tmp_path)
    monkeypatch.setattr(path_config, "DEFAULT_CONFIG_PATH", local_config)
    monkeypatch.setattr(path_config, "EXAMPLE_CONFIG_PATH", config_dir / "example_paths.yaml")

    paths = load_path_config("configlocal_paths.yaml")

    assert paths.config_path == local_config.resolve()
    assert paths.raw_data_root == raw_root.resolve()
