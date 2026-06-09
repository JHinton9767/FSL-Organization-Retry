from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from time import perf_counter

import pandas as pd

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[2]))

from src.graduation_pipeline.apply_corrections import ensure_manual_corrections_file, load_manual_corrections
from src.graduation_pipeline.config import GraduationPipelineConfig, load_graduation_config
from src.graduation_pipeline.graduation_evidence import build_graduation_evidence
from src.graduation_pipeline.graduation_rates import build_final_outcomes, build_qa_summary, build_rates
from src.graduation_pipeline.load_required_fields import load_required_fields
from src.graduation_pipeline.manual_review import build_manual_review_queue
from src.graduation_pipeline.membership_summary import build_membership_summary
from src.graduation_pipeline.normalize import normalize_required_fields
from src.graduation_pipeline.source_inventory import build_source_manifest, manifest_digest


def _write_frame(path: Path, frame: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(path, index=False)
    if path.suffix.lower() == ".csv":
        parquet_path = path.with_suffix(".parquet")
        try:
            frame.to_parquet(parquet_path, index=False)
        except Exception:
            pass


def _read_cached_frame(path: Path) -> pd.DataFrame | None:
    parquet_path = path.with_suffix(".parquet")
    if parquet_path.exists():
        try:
            return pd.read_parquet(parquet_path)
        except Exception:
            pass
    if path.exists():
        return pd.read_csv(path, dtype=str, keep_default_na=False)
    return None


def run(config: GraduationPipelineConfig, refresh_cache: bool = False) -> dict[str, Path]:
    started = perf_counter()
    ensure_manual_corrections_file(config.manual_corrections_path)
    output = config.output_root
    cache = config.cache_root
    output.mkdir(parents=True, exist_ok=True)
    cache.mkdir(parents=True, exist_ok=True)

    manifest = build_source_manifest(config)
    digest = manifest_digest(manifest)
    meta_path = cache / "manifest_meta.json"
    raw_cache_path = cache / "raw_required_fields.csv"
    cached_digest = ""
    if meta_path.exists():
        try:
            cached_digest = json.loads(meta_path.read_text(encoding="utf-8")).get("manifest_digest", "")
        except Exception:
            cached_digest = ""

    raw = None
    if not refresh_cache and cached_digest == digest:
        raw = _read_cached_frame(raw_cache_path)
    if raw is None:
        raw = load_required_fields(manifest)
        _write_frame(raw_cache_path, raw)
        meta_path.write_text(json.dumps({"manifest_digest": digest}, indent=2), encoding="utf-8")

    normalized, invalid_ids = normalize_required_fields(raw)
    membership = build_membership_summary(normalized)
    evidence = build_graduation_evidence(normalized)
    manual_queue = build_manual_review_queue(membership, evidence, invalid_ids)
    corrections = load_manual_corrections(config.manual_corrections_path)
    final, applied, audit = build_final_outcomes(membership, evidence, manual_queue, corrections)

    outputs = {
        "source_manifest": output / "source_manifest.csv",
        "raw_required_fields": output / "raw_required_fields.csv",
        "normalized_required_fields": output / "normalized_required_fields.csv",
        "invalid_ids": output / "invalid_ids.csv",
        "student_membership_summary": output / "student_membership_summary.csv",
        "graduation_evidence": output / "graduation_evidence.csv",
        "manual_review_queue": output / "manual_review_queue.csv",
        "manual_corrections_applied": output / "manual_corrections_applied.csv",
        "correction_audit": output / "correction_audit.csv",
        "final_student_outcomes": output / "final_student_outcomes.csv",
        "graduation_rates_by_cohort": output / "graduation_rates_by_cohort.csv",
        "graduation_rates_by_chapter": output / "graduation_rates_by_chapter.csv",
        "graduation_rates_by_council": output / "graduation_rates_by_council.csv",
        "graduation_rate_qa_summary": output / "graduation_rate_qa_summary.csv",
    }

    _write_frame(outputs["source_manifest"], manifest)
    _write_frame(outputs["raw_required_fields"], raw)
    _write_frame(outputs["normalized_required_fields"], normalized)
    _write_frame(outputs["invalid_ids"], invalid_ids)
    _write_frame(outputs["student_membership_summary"], membership)
    _write_frame(outputs["graduation_evidence"], evidence)
    _write_frame(outputs["manual_review_queue"], manual_queue)
    _write_frame(outputs["manual_corrections_applied"], applied)
    _write_frame(outputs["correction_audit"], audit)
    _write_frame(outputs["final_student_outcomes"], final)
    _write_frame(outputs["graduation_rates_by_cohort"], build_rates(final, ["cohort_term_code", "cohort_term"]))
    _write_frame(outputs["graduation_rates_by_chapter"], build_rates(final, ["cohort_term_code", "cohort_term", "chapter"]))
    _write_frame(outputs["graduation_rates_by_council"], build_rates(final, ["cohort_term_code", "cohort_term", "council"]))
    _write_frame(outputs["graduation_rate_qa_summary"], build_qa_summary(final, invalid_ids, manual_queue, corrections))

    perf = pd.DataFrame(
        [
            {"stage": "total", "seconds": round(perf_counter() - started, 3), "rows": len(final), "notes": "focused graduation pipeline"},
            {"stage": "sources", "seconds": "", "rows": len(manifest), "notes": f"manifest digest {digest}"},
        ]
    )
    _write_frame(output / "performance_report.csv", perf)
    return outputs


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run the focused FSL graduation-rate pipeline.")
    parser.add_argument("--config", default=None, help="Optional config/local_paths.yaml override.")
    parser.add_argument("--refresh-cache", action="store_true", help="Rescan sources even if the source manifest is unchanged.")
    args = parser.parse_args(argv)
    config = load_graduation_config(args.config)
    outputs = run(config, refresh_cache=args.refresh_cache)
    print(f"Wrote graduation outputs to {config.output_root}")
    print(f"Manual corrections file: {config.manual_corrections_path}")
    print(f"Final outcomes: {outputs['final_student_outcomes']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

