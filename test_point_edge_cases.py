"""
tests/test_point_edge_cases.py

Edge case tests for PointType covering four real-world failure modes:

  1. Precision round-trip drift  – high-precision floats may lose bits when
     PostgreSQL serializes them as text and we parse them back.
  2. Exact boundary values       – ±180 / ±90 must pass validation; values
     just outside must not. Float representation must not silently nudge a
     valid boundary into an invalid one on round-trip.
  3. Anti-meridian coordinates   – values near ±180° are legal and must
     survive bind → result round-trips intact.
  4. Scientific notation strings – PostgreSQL can emit "1e-10" for near-zero
     coordinates. The current result_processor regex rejects this silently,
     which is a real bug this test suite exists to catch and document.

All tests are offline (no database required) — they exercise the processors
directly, keeping CI fast and the intent readable.
"""

import math
import re

import pytest

from sqlalchemy_postgres_point import PointType


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def bind(value):
    """Run a value through the bind processor (Python → DB string)."""
    return PointType().bind_processor(None)(value)


def result(value):
    """Run a DB string through the result processor (DB string → Python tuple)."""
    return PointType().result_processor(None, None)(value)


# ---------------------------------------------------------------------------
# 1. Precision round-trip drift
#
# PostgreSQL serializes POINT as text with enough digits to round-trip a
# float64 exactly (it uses the shortest decimal representation since PG 12).
# We verify that a high-precision coordinate survives bind → result without
# meaningful loss. Tolerance is 1 ULP (unit in the last place) at 1e-10 scale.
# ---------------------------------------------------------------------------

PRECISION_CASES = [
    # (lng, lat, description)
    (1.123456789012345,   -45.987654321098765, "high-precision real-world-ish coord"),
    (0.000000001,          0.000000001,        "near-zero positive"),
    (-0.000000001,        -0.000000001,        "near-zero negative"),
    (123.456789012345678,  67.891234567890123, "many significant digits"),
]


@pytest.mark.parametrize("lng,lat,desc", PRECISION_CASES)
def test_precision_roundtrip(lng, lat, desc):
    """
    Bind a high-precision coord to a string, then parse that string back.
    The recovered tuple must be within 1e-10 of the original values.
    This catches any truncation introduced by our own formatting in
    bind_processor (f"{lng},{lat}") — Python's default float repr is
    lossless for float64, so this should always pass.  If it ever fails,
    we introduced a format change that drops precision.
    """
    db_string = bind((lng, lat))
    recovered_lng, recovered_lat = result(db_string)

    assert abs(recovered_lng - lng) < 1e-10, (
        f"[{desc}] lng drift: {lng!r} → {recovered_lng!r}"
    )
    assert abs(recovered_lat - lat) < 1e-10, (
        f"[{desc}] lat drift: {lat!r} → {recovered_lat!r}"
    )


# ---------------------------------------------------------------------------
# 2. Exact boundary values
#
# ±180 / ±90 are valid per WGS-84 and must pass _validate_point. Values
# just outside (even by the smallest representable float step) must raise.
# We also verify the boundary survives our own bind → result round-trip so
# float representation doesn't silently bump a legal value out of range.
# ---------------------------------------------------------------------------

VALID_BOUNDARIES = [
    (180.0,   90.0,  "northeast corner"),
    (-180.0, -90.0,  "southwest corner"),
    (0.0,     0.0,   "null island"),
    (180.0,   0.0,   "antimeridian equator"),
    (0.0,    90.0,   "north pole"),
    (0.0,   -90.0,   "south pole"),
]

INVALID_JUST_OUTSIDE = [
    # math.nextafter gives the smallest float step beyond the boundary
    (math.nextafter(180.0, math.inf),   0.0,  "lng > 180 by 1 ULP"),
    (math.nextafter(-180.0, -math.inf), 0.0,  "lng < -180 by 1 ULP"),
    (0.0,  math.nextafter(90.0, math.inf),   "lat > 90 by 1 ULP"),
    (0.0,  math.nextafter(-90.0, -math.inf), "lat < -90 by 1 ULP"),
]


@pytest.mark.parametrize("lng,lat,desc", VALID_BOUNDARIES)
def test_exact_boundary_is_valid(lng, lat, desc):
    """Exact boundary coordinates must bind without raising."""
    db_string = bind((lng, lat))
    assert db_string is not None, f"[{desc}] bind returned None unexpectedly"


@pytest.mark.parametrize("lng,lat,desc", VALID_BOUNDARIES)
def test_exact_boundary_roundtrip(lng, lat, desc):
    """
    Boundary values must survive bind → result intact.
    A float formatting bug could turn 180.0 into "180.00000000000003"
    which would then fail the range check on the result side.
    """
    db_string = bind((lng, lat))
    recovered = result(db_string)
    assert recovered == (lng, lat), (
        f"[{desc}] roundtrip changed value: {(lng, lat)!r} → {recovered!r}"
    )


@pytest.mark.parametrize("lng,lat,desc", INVALID_JUST_OUTSIDE)
def test_just_outside_boundary_is_rejected(lng, lat, desc):
    """Values one ULP outside the valid range must raise ValueError."""
    with pytest.raises(ValueError, match=r"(Longitude|Latitude) must be within"):
        bind((lng, lat))


# ---------------------------------------------------------------------------
# 3. Anti-meridian coordinates
#
# The anti-meridian (±180°) is a common pain point in geospatial code.
# Applications handling international date line crossings regularly store
# coordinates at or near ±180. Verify these are accepted and stable.
# ---------------------------------------------------------------------------

ANTI_MERIDIAN_CASES = [
    (179.9999999,  45.0,  "just west of antimeridian"),
    (-179.9999999, 45.0,  "just east of antimeridian"),
    (180.0,        0.0,   "exactly on antimeridian"),
    (-180.0,       0.0,   "exactly on antimeridian negative"),
    (179.99999999999997, 0.0, "antimeridian minus 1 ULP (still valid)"),
]


@pytest.mark.parametrize("lng,lat,desc", ANTI_MERIDIAN_CASES)
def test_antimeridian_bind_and_result(lng, lat, desc):
    """
    Anti-meridian coordinates must bind cleanly and parse back to the same
    value. We check both directions to catch sign-flip bugs.
    """
    db_string = bind((lng, lat))
    recovered_lng, recovered_lat = result(db_string)

    assert abs(recovered_lng - lng) < 1e-10, (
        f"[{desc}] lng changed: {lng!r} → {recovered_lng!r}"
    )
    assert abs(recovered_lat - lat) < 1e-10, (
        f"[{desc}] lat changed: {lat!r} → {recovered_lat!r}"
    )


# ---------------------------------------------------------------------------
# 4. Scientific notation in result strings — known bug
#
# PostgreSQL can emit coordinates in scientific notation for very small values
# (e.g. "1e-10"). The current result_processor regex only matches digits and
# dots, so it raises ValueError on these strings instead of parsing them.
#
# These tests are marked xfail to document the known limitation without
# blocking CI. When the bug is fixed (by upgrading the regex to handle
# scientific notation), change xfail → regular passing tests.
# ---------------------------------------------------------------------------

SCI_NOTATION_CASES = [
    ("(1e-10,2e-10)",          "positive scientific notation"),
    ("(-1e-10,-2e-10)",        "negative scientific notation"),
    ("(1.5e-7,3.2e-8)",        "scientific with decimal"),
    ("(1E-10,2E-10)",          "uppercase E"),
    ("(1.23e+2,4.56e+1)",      "positive exponent — 123.0, 45.6"),
]

# Regex that WOULD correctly handle scientific notation (for reference in fix):
# r"^\((-?[\d.]+(?:[eE][+-]?\d+)?),(-?[\d.]+(?:[eE][+-]?\d+)?)\)$"
_SCI_REGEX = re.compile(
    r"^\((-?[\d.]+(?:[eE][+-]?\d+)?),(-?[\d.]+(?:[eE][+-]?\d+)?)\)$"
)


@pytest.mark.parametrize("db_string,desc", SCI_NOTATION_CASES)
@pytest.mark.xfail(
    reason=(
        "result_processor regex does not handle scientific notation from PostgreSQL. "
        "Fix: extend regex to r'(-?[\\d.]+(?:[eE][+-]?\\d+)?)' — see test file comment."
    ),
    strict=True,  # must fail; if it passes unexpectedly that's also a signal
)
def test_scientific_notation_is_parsed(db_string, desc):
    """
    PostgreSQL may return POINT coordinates in scientific notation.
    The result_processor must parse these without raising ValueError.
    Currently xfail — this is the bug being documented.
    """
    parsed = result(db_string)
    assert parsed is not None, f"[{desc}] result was None for {db_string!r}"
    assert len(parsed) == 2, f"[{desc}] expected 2-tuple, got {parsed!r}"


def test_scientific_notation_regex_itself_works():
    """
    Confirm the corrected regex (not yet used in production code) correctly
    matches scientific notation strings. This test always passes and serves
    as the spec for the fix — copy this regex into result_processor to resolve
    the xfail tests above.
    """
    valid_sci_strings = [
        "(1e-10,2e-10)",
        "(-1e-10,-2e-10)",
        "(1.5e-7,3.2e-8)",
        "(1E-10,2E-10)",
        "(1.23e+2,4.56e+1)",
        # Normal strings must still match
        "(1.23,4.56)",
        "(-180.0,90.0)",
        "(0.0,0.0)",
    ]
    for s in valid_sci_strings:
        assert _SCI_REGEX.match(s), f"Fixed regex did not match: {s!r}"
