import json

import pytest

from pipeline import config as C
from pipeline import checkpoint_report


def write_json(path, value):
    path.write_text(json.dumps(value), encoding="utf-8")


def test_report_requires_successful_pipeline_steps(tmp_path, monkeypatch):
    results = tmp_path / "results"
    results.mkdir()
    write_json(results / "pipeline_run.json", {"steps": {"clean": {"status": "failed"}}})
    monkeypatch.setattr(C, "RESULTS_DIR", results)

    with pytest.raises(RuntimeError, match="unsuccessful steps"):
        checkpoint_report.main()


def test_report_uses_current_metrics_and_samples(tmp_path, monkeypatch):
    results = tmp_path / "results"
    reports = tmp_path / "reports"
    results.mkdir()
    evidence = reports / "evidence"
    evidence.mkdir(parents=True)
    monkeypatch.setattr(C, "RESULTS_DIR", results)
    monkeypatch.setattr(C, "REPORTS", reports)
    monkeypatch.setattr(C, "MANIFEST", tmp_path / "manifest.jsonl")
    C.MANIFEST.write_text('{"kind":"hourly","n_records":8}\n', encoding="utf-8")
    (evidence / "clean_plan.txt").write_text("formatted plan", encoding="utf-8")

    names = ("clean", "analysis", "advanced", "verify", "benchmark", "figures")
    write_json(results / "pipeline_run.json", {
        "steps": {
            **{name: {"status": "ok", "elapsed_s": 1.0} for name in names},
            "report": {"status": "failed", "elapsed_s": 0.1},
        }
    })
    write_json(results / "clean_stats.json", {
        "raw_rows": 8, "rows_after_deduplicate": 8, "duplicates_removed": 0,
        "location_dimension_keys": 1, "rows_unmatched_location_dimension": 0,
        "rows_before_grid_join": 8, "rows_matched_to_grid": 8, "rows_outside_grid": 0,
        "expected_grid_rows": 8, "rows_after_dimension_join": 8, "grid_join_row_delta": 0,
        "missing_rows": 0, "rows_imputed": 0, "out_of_range_total": 0,
        "null_temperature_after_clean": 0, "null_precipitation_after_clean": 0,
        "output_schema": {"fields": [{"name": "location_id", "type": "string", "nullable": False}]},
        "clean_rows": 8, "expected_rows": 8, "parquet_roundtrip_ok": True,
    })
    write_json(results / "verification_report.json", {
        "all_passed": True, "V1_sample": {"passed": True}
    })
    write_json(results / "benchmark.json", {
        "B1_storage_MB": {"csv_uncompressed": 1.0, "parquet_snappy_flat": 0.5},
        "B2_q1_seconds": {"csv": [1.0, [1.0]], "parquet": [0.5, [0.5]]},
    })
    for name in (
        "q1_monthly_temp_core.csv",
        "q2_monthly_precip_core.csv",
        "q3_top5_rain_days_core.csv",
    ):
        (results / name).write_text("sample\nvalue\n", encoding="utf-8")

    checkpoint_report.main()
    report = (reports / "checkpoint3.md").read_text(encoding="utf-8")
    assert "8 bản ghi trong manifest" in report
    assert "matched input rows: 8" in report
    assert "| sample |" in report
    assert "`report` | failed" not in report
