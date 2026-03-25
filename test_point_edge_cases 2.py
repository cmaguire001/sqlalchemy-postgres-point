"""
tests/test_point_edge_cases.py

Edge case tests for PointType covering five real-world failure modes:

  1. Precision round-trip drift  – high-precision floats may lose bits when
     PostgreSQL serializes them as text and we parse them back.
  2. Exact boundary values       – ±180 / ±90 must pass validation; values
     just outside must not.
  3. Anti-meridian coordinates   – values near ±180° are legal and must
     survive bind → result round-trips intact.
  4. Scientific notation strings – PostgreSQL can emit "1e-10" for near-zero
     coordinates. The current result_processor regex rejects this silently,
     which is a real bug this test suite exists to catch and document.
  5. Swapped lat/lng detection   – (lat, lng) passed instead of (lng, lat)
     is the most common silent migration error. Detectable when lng > 90,
     undetectable (documented blind spot) when both values fit in [-90, 90].

All tests are offline (no database required).
"""

import math
import re

import pytest

from sqlalchemy_postgres_point import PointType


# ---------------------------------------------------------------------------
# Helpers — used by all sections below
# ---------------------------------------------------------------------------

def bind(value):
    """Run a value through the bind processor (Python tuple → DB string)."""
    return PointType().bind_processor(None)(value)


def result(value):
    """Run a DB string through the result processor (DB string → Python tuple)."""
    return PointType().result_processor(None, None)(value)


# ---------------------------------------------------------------------------
# 1. Precision round-trip drift
# ---------------------------------------------------------------------------

PRECISION_CASES = [
    (1.123456789012345,   -45.987654321098765, "high-precision real-world coord"),
    (0.000000001,          0.000000001,        "near-zero positive"),
    (-0.000000001,        -0.000000001,        "near-zero negative"),
    (123.456789012345678,  67.891234567890123, "many significant digits"),
]


@pytest.mark.parametrize("lng,lat,desc", PRECISION_CASES)
def test_precision_roundtrip(lng, lat, desc):
    """High-precision float64 coords must survive bind → result within 1e-10."""
    db_string = bind((lng, lat))
    recovered_lng, recovered_lat = result(db_string)
    assert abs(recovered_lng - lng) < 1e-10, f"[{desc}] lng drift: {lng!r} → {recovered_lng!r}"
    assert abs(recovered_lat - lat) < 1e-10, f"[{desc}] lat drift: {lat!r} → {recovered_lat!r}"


# ---------------------------------------------------------------------------
# 2. Exact boundary values
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
    (math.nextafter(180.0, math.inf),    0.0, "lng > 180 by 1 ULP"),
    (math.nextafter(-180.0, -math.inf),  0.0, "lng < -180 by 1 ULP"),
    (0.0,  math.nextafter(90.0, math.inf),   "lat > 90 by 1 ULP"),
    (0.0,  math.nextafter(-90.0, -math.inf), "lat < -90 by 1 ULP"),
]


@pytest.mark.parametrize("lng,lat,desc", VALID_BOUNDARIES)
def test_exact_boundary_is_valid(lng, lat, desc):
    """Exact boundary coordinates must bind without raising."""
    assert bind((lng, lat)) is not None, f"[{desc}] bind returned None unexpectedly"


@pytest.mark.parametrize("lng,lat,desc", VALID_BOUNDARIES)
def test_exact_boundary_roundtrip(lng, lat, desc):
    """Boundary values must survive bind → result without float drift."""
    assert result(bind((lng, lat))) == (lng, lat)


@pytest.mark.parametrize("lng,lat,desc", INVALID_JUST_OUTSIDE)
def test_just_outside_boundary_is_rejected(lng, lat, desc):
    """Values one ULP outside valid range must raise ValueError."""
    with pytest.raises(ValueError, match=r"(Longitude|Latitude) must be within"):
        bind((lng, lat))


# ---------------------------------------------------------------------------
# 3. Anti-meridian coordinates
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
    """Anti-meridian coordinates must bind and parse back intact."""
    recovered_lng, recovered_lat = result(bind((lng, lat)))
    assert abs(recovered_lng - lng) < 1e-10, f"[{desc}] lng changed: {lng!r} → {recovered_lng!r}"
    assert abs(recovered_lat - lat) < 1e-10, f"[{desc}] lat changed: {lat!r} → {recovered_lat!r}"


# ---------------------------------------------------------------------------
# 4. Scientific notation — known bug (xfail)
#
# PostgreSQL can emit "1e-10" for near-zero coordinates. The current regex
# only matches digits and dots, so it raises ValueError on these strings.
# Fix: extend the regex to handle scientific notation (see comment below).
# ---------------------------------------------------------------------------

SCI_NOTATION_CASES = [
    ("(1e-10,2e-10)",     "positive scientific notation"),
    ("(-1e-10,-2e-10)",   "negative scientific notation"),
    ("(1.5e-7,3.2e-8)",   "scientific with decimal"),
    ("(1E-10,2E-10)",     "uppercase E"),
    ("(1.23e+2,4.56e+1)", "positive exponent — 123.0, 45.6"),
]

# One-line fix for point.py result_processor — swap current regex for this:
# r"^\((-?[\d.]+(?:[eE][+-]?\d+)?),(-?[\d.]+(?:[eE][+-]?\d+)?)\)$"
_SCI_REGEX = re.compile(
    r"^\((-?[\d.]+(?:[eE][+-]?\d+)?),(-?[\d.]+(?:[eE][+-]?\d+)?)\)$"
)


@pytest.mark.parametrize("db_string,desc", SCI_NOTATION_CASES)

def test_scientific_notation_is_parsed(db_string, desc):
    """PostgreSQL may return POINT coords in scientific notation — must not raise."""
    parsed = result(db_string)
    assert parsed is not None
    assert len(parsed) == 2


def test_scientific_notation_regex_itself_works():
    """The corrected regex matches all scientific notation forms — spec for the fix."""
    for s in [s for s, _ in SCI_NOTATION_CASES] + ["(1.23,4.56)", "(-180.0,90.0)"]:
        assert _SCI_REGEX.match(s), f"Fixed regex did not match: {s!r}"


# ---------------------------------------------------------------------------
# 5. Swapped lat/lng detection — silent migration error
#
# PointType expects (lng, lat). Passing (lat, lng) by mistake is the most
# common geospatial migration bug. It is detectable when the true longitude
# exceeds 90 (Asia, Pacific, Australia) because that value is out of range
# in the lat position. It is undetectable when both values fit in [-90, 90]
# (most of Europe, Africa, Americas) — that blind spot is documented below.
# ---------------------------------------------------------------------------

DETECTABLE_SWAPS = [
    # true lng > 90 → lands out of lat range when swapped → raises
    (35.6895,  139.6917, "Tokyo — lng 139 out of lat range"),
    (-33.8688, 151.2093, "Sydney — lng 151 out of lat range"),
    (1.3521,   103.8198, "Singapore — lng 103 out of lat range"),
]

AMBIGUOUS_SWAPS = [
    # true lng within [-90, 90] → both orderings numerically valid → silent
    (51.5074,  -0.1278, "London — both values within [-90,90]"),
    (48.8566,   2.3522, "Paris — both values within [-90,90]"),
]


@pytest.mark.parametrize("lat,lng,desc", DETECTABLE_SWAPS)
def test_detectable_swap_raises(lat, lng, desc):
    """
    When the true longitude > 90 is passed in the lat position, _validate_point
    must raise. This catches swapped coords for Asia, Pacific, and Australia.
    """
    with pytest.raises(ValueError, match=r"(Longitude|Latitude) must be within"):
        bind((lat, lng))  # intentionally swapped: lat first, lng second


@pytest.mark.parametrize("lat,lng,desc", AMBIGUOUS_SWAPS)
def test_ambiguous_swap_is_silently_accepted(lat, lng, desc):
    """
    Documents the known blind spot: when both lat and lng fall within [-90, 90],
    PointType cannot detect a swap — both orderings are valid numbers.
    Detection must happen at the application layer, not here.
    """
    assert bind((lng, lat)) is not None  # correct order
    assert bind((lat, lng)) is not None  # swapped — silently accepted (blind spot)
