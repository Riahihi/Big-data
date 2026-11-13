from pathlib import Path

import pytest

from pipeline.benchmark import load_average, local_file_path
from pipeline.clean import validate_join_quality, validate_location_keys


def test_spark_file_uri_maps_to_native_windows_path():
    assert local_file_path("file:///C:/data/hourly%20file.parquet") == Path(
        "C:/data/hourly file.parquet"
    )


def test_load_average_is_explicitly_unavailable_when_not_supported(monkeypatch):
    monkeypatch.delattr("pipeline.benchmark.os.getloadavg", raising=False)
    assert load_average() is None


def test_location_dimension_rejects_duplicate_and_unmatched_keys():
    with pytest.raises(ValueError, match="duplicate keys"):
        validate_location_keys({"HNI"}, [("HNI",), ("HNI",)])
    with pytest.raises(ValueError, match="missing from location dimension"):
        validate_location_keys({"XYZ"}, [("HNI",)])


def test_join_quality_rejects_row_count_delta():
    with pytest.raises(ValueError, match="grid join changed row count"):
        validate_join_quality({
            "rows_outside_grid": 0,
            "rows_unmatched_location_dimension": 0,
            "grid_join_row_delta": -1,
        })
