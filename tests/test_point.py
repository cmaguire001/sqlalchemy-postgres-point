import re
from pathlib import Path

import pytest

sqlalchemy = pytest.importorskip("sqlalchemy")
from sqlalchemy.dialects import postgresql
from sqlalchemy.sql import column, select

from sqlalchemy_postgres_point import PointType, analyze_point, validate_points


def test_bind_processor():
    pt = PointType()
    proc = pt.bind_processor(None)
    assert proc((1.23, 4.56)) == "(1.23,4.56)"
    assert proc(None) is None


def test_literal_processor():
    pt = PointType()
    proc = pt.literal_processor(None)
    assert proc((1.23, 4.56)) == "'(1.23,4.56)'"
    assert proc(None) == "NULL"


def test_result_processor_valid():
    pt = PointType()
    proc = pt.result_processor(None, None)
    assert proc("(1.23,4.56)") == (1.23, 4.56)
    assert proc(None) is None


def test_result_processor_invalid():
    pt = PointType()
    proc = pt.result_processor(None, None)
    with pytest.raises(ValueError):
        proc("invalid")


def test_validation_bind_out_of_range():
    pt = PointType()
    bind = pt.bind_processor(None)
    with pytest.raises(ValueError):
        bind((200.0, 0.0))  # invalid longitude
    with pytest.raises(ValueError):
        bind((0.0, 100.0))  # invalid latitude


def test_validation_literal_non_finite():
    import math

    pt = PointType()
    lit = pt.literal_processor(None)
    with pytest.raises(ValueError):
        lit((math.nan, 0.0))
    with pytest.raises(ValueError):
        lit((0.0, math.inf))


def test_validation_result_out_of_range():
    pt = PointType()
    res = pt.result_processor(None, None)
    with pytest.raises(ValueError):
        res("(181,0)")
    with pytest.raises(ValueError):
        res("(0,91)")


def test_earth_distance_compilation():
    # Build an expression using the custom comparator
    c = column("location", PointType())
    other_point = (10.0, 20.0)
    expr = c.earth_distance(other_point)
    stmt = select(expr)
    compiled = stmt.compile(dialect=postgresql.dialect())
    sql = str(compiled)
    # Expect the <@> operator to appear between the column and a bind / literal
    assert "<@>" in sql
    # Ensure the POINT literal/bind shape is present
    assert re.search(r"location\s*<@>", sql)


def test_result_processor_three_values():
    """Three-value string should raise clear Invalid POINT value error, not
    confusing 'could not convert string to float' from the old greedy regex."""
    pt = PointType()
    proc = pt.result_processor(None, None)
    with pytest.raises(ValueError, match="Invalid POINT value"):
        proc("(10.0,45.0,99.0)")


def test_result_processor_non_numeric():
    """Non-numeric coordinates should raise clear Invalid POINT value error."""
    pt = PointType()
    proc = pt.result_processor(None, None)
    with pytest.raises(ValueError, match="Invalid POINT value"):
        proc("(abc,def)")


def test_bind_processor_non_numeric():
    """Non-numeric coordinates in bind processor should raise clear error."""
    pt = PointType()
    proc = pt.bind_processor(None)
    with pytest.raises(ValueError, match="Point coordinates must be numeric"):
        proc(("abc", "def"))


def test_result_processor_geonames_andorra():
    """
    Round-trip every coordinate in the GeoNames Andorra dump through the
    result processor offline — no database required.
    Downloads automatically if not already present.
    """
    import csv
    import io
    import os
    import urllib.request
    import zipfile

    geonames_path = "/tmp/geonames/AD.txt"
    if not os.path.exists(geonames_path):
        os.makedirs("/tmp/geonames", exist_ok=True)
        url = "https://download.geonames.org/export/dump/AD.zip"
        with urllib.request.urlopen(url) as response:
            zip_data = response.read()
        with zipfile.ZipFile(io.BytesIO(zip_data)) as zf:
            with zf.open("AD.txt") as src, open(geonames_path, "wb") as dst:
                dst.write(src.read())

    pt = PointType()
    proc = pt.result_processor(None, None)

    errors = []
    with open(geonames_path, encoding="utf-8") as f:
        reader = csv.reader(f, delimiter="\t")
        for i, row in enumerate(reader):
            try:
                lat, lng = row[4], row[5]
                point_str = f"({lng},{lat})"
                result = proc(point_str)
                assert isinstance(result, tuple)
                assert len(result) == 2
            except Exception as e:
                errors.append((i, row[4], row[5], str(e)))

    assert errors == [], f"{len(errors)} rows failed:\n" + "\n".join(
        f"  row {i}: ({lat},{lng}) -> {err}" for i, lat, lng, err in errors
    )


def test_analyze_point_ambiguous_confidence():
    analysis = PointType()._validate_point((45.0, 30.0))
    assert analysis == (45.0, 30.0)

    result = analyze_point((45.0, 30.0))
    assert result.valid is True
    assert result.ambiguous is True
    assert 0.0 <= result.confidence <= 0.62


def test_strict_mode_warns_on_ambiguous():
    pt = PointType(strict=True, strict_mode="warn")
    proc = pt.bind_processor(None)
    with pytest.warns(UserWarning, match="Ambiguous POINT coordinates"):
        assert proc((20.0, 30.0)) == "(20.0,30.0)"


def test_strict_mode_error_on_ambiguous():
    pt = PointType(strict=True, strict_mode="error")
    proc = pt.bind_processor(None)
    with pytest.raises(ValueError, match="Ambiguous POINT coordinates"):
        proc((20.0, 30.0))


def test_strict_mode_logger_warning_on_ambiguous(caplog):
    import logging

    logger = logging.getLogger("sqlalchemy_postgres_point.tests")
    pt = PointType(strict=True, strict_mode="warn", logger=logger)
    proc = pt.bind_processor(None)

    with caplog.at_level(logging.WARNING):
        assert proc((20.0, 30.0)) == "(20.0,30.0)"

    assert "Ambiguous POINT coordinates detected" in caplog.text


def test_validate_points_batch_summary():
    summary = validate_points(
        [
            (1.0, 2.0),
            (40.0, -120.0),  # likely swap (lat,lng)
            (181.0, 0.0),  # invalid longitude
            (50.0, 20.0),  # ambiguous
        ]
    )

    assert summary["total_rows"] == 4
    assert summary["likely_swapped_pct"] == 25.0
    assert summary["invalid_pct"] == 25.0
    assert summary["ambiguous_pct"] == 50.0
    assert len(summary["flagged_rows"]) >= 3
    assert isinstance(summary["flagged_rows"][0]["analysis"], dict)


def test_point_type_rejects_invalid_strict_mode():
    with pytest.raises(ValueError, match="strict_mode must be either"):
        PointType(strict=True, strict_mode="log")


def test_result_processor_osm_raw_extract_nodes():
    """
    Parse a raw OSM XML extract and validate every node coordinate through
    PointType's result processor without requiring a database connection.
    """
    import xml.etree.ElementTree as ET

    osm_path = Path(__file__).parent / "data" / "osm_sample_extract.osm"
    tree = ET.parse(osm_path)
    root = tree.getroot()

    pt = PointType()
    proc = pt.result_processor(None, None)

    parsed = 0
    for node in root.findall("node"):
        lat = node.attrib["lat"]
        lng = node.attrib["lon"]
        point_str = f"({lng},{lat})"
        result = proc(point_str)
        assert isinstance(result, tuple)
        assert len(result) == 2
        parsed += 1

    assert parsed > 0


@pytest.mark.integration
def test_result_processor_osm_raw_extract_remote():
    """
    Download a small raw OSM extract (Andorra) and validate the first batch of
    node coordinates through the result processor. Skips when network is blocked.
    """
    import bz2
    import io
    import urllib.error
    import urllib.request
    import xml.etree.ElementTree as ET

    url = "https://download.geofabrik.de/europe/andorra-latest.osm.bz2"
    try:
        with urllib.request.urlopen(url, timeout=60) as response:
            compressed = response.read()
    except (urllib.error.URLError, TimeoutError):
        pytest.skip("network blocked: unable to fetch geofabrik raw extract")

    decompressed = bz2.decompress(compressed)

    pt = PointType()
    proc = pt.result_processor(None, None)

    parsed = 0
    for _, elem in ET.iterparse(io.BytesIO(decompressed), events=("end",)):
        if elem.tag != "node":
            continue

        lat = elem.attrib.get("lat")
        lng = elem.attrib.get("lon")
        if lat is None or lng is None:
            continue

        result = proc(f"({lng},{lat})")
        assert isinstance(result, tuple)
        assert len(result) == 2

        parsed += 1
        if parsed >= 5000:
            break

    assert parsed > 0
