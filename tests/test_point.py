import re

import pytest
from sqlalchemy.dialects import postgresql
from sqlalchemy.sql import column, select

from sqlalchemy_postgres_point import PointType


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
