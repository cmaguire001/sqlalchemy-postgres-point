import math
import re
import pytest

from sqlalchemy_postgres_point import PointType

def bind(value):
    return PointType().bind_processor(None)(value)

def result(value):
    return PointType().result_processor(None, None)(value)

import pytest


# ---------------------------------------------------------------------------
# 5. Swapped lat/lng detection — silent migration error
#
# Convention: PointType expects (lng, lat). A common migration bug passes
# (lat, lng) instead. If lat is in [-90,90] and lng is in (90,180], the
# swap is undetectable — both orderings are geometrically valid numbers.
# But if the true longitude exceeds 90 (i.e. most of Asia, Pacific, Australia)
# the swapped value lands in lat position outside [-90,90] and raises.
#
# We also add an explicit warning for the ambiguous zone where both orderings
# are numerically valid — that case can only be caught at the app layer.
# ---------------------------------------------------------------------------

DETECTABLE_SWAPS = [
    # (wrongly_passed_lat, wrongly_passed_lng, desc)
    # These will raise because the true lng > 90, so when swapped into lat position it's out of range
    (40.7128,  -74.0060, "New York — lng -74 is valid lat, but swap detectable if user passes backwards"),
    (35.6895,  139.6917, "Tokyo — lng 139 in lat position raises immediately"),
    (-33.8688, 151.2093, "Sydney — lng 151 in lat position raises immediately"),
    (1.3521,   103.8198, "Singapore — lng 103 in lat position raises"),
    (64.1466,  -21.9426, "Reykjavik — detectable only if lng passed as lat exceeds 90"),
]

AMBIGUOUS_SWAPS = [
    # Both (lat, lng) and (lng, lat) are numerically valid — undetectable at type layer
    # lng is within [-90, 90] so it looks like a valid lat either way
    (40.7128,  -74.0060, "NYC lng -74 fits in lat range — ambiguous"),
    (51.5074,   -0.1278, "London — both values within [-90,90] — ambiguous"),
    (48.8566,    2.3522, "Paris — both values within [-90,90] — ambiguous"),
]


@pytest.mark.parametrize("lat,lng,desc", DETECTABLE_SWAPS)
def test_detectable_swap_raises(lat, lng, desc):
    """
    When a true longitude > 90 is accidentally passed in the lat position,
    _validate_point must raise ValueError. This catches the most dangerous
    class of swap — coordinates in Asia, Pacific, Australia, Americas > 90W.
    """
    if abs(lng) <= 90:
        pytest.skip(f"[{desc}] lng {lng} fits in lat range — ambiguous, not detectable at type layer")

    with pytest.raises(ValueError, match=r"(Longitude|Latitude) must be within"):
        bind((lat, lng))  # intentionally swapped: lat first, lng second


@pytest.mark.parametrize("lat,lng,desc", AMBIGUOUS_SWAPS)
def test_ambiguous_swap_is_silently_accepted(lat, lng, desc):
    """
    Documents the known limitation: when both lat and lng fall within [-90,90],
    a swap is geometrically undetectable at the type layer. This test passes
    (no error raised) to make the blind spot explicit in the test suite.
    The fix belongs in application-layer validation, not PointType.
    """
    # Both orderings are valid numbers — PointType cannot know which is correct
    result_correct = bind((lng, lat))   # correct:  (lng, lat)
    result_swapped = bind((lat, lng))   # swapped:  (lat, lng) — silently accepted

    assert result_correct is not None
    assert result_swapped is not None  # this is the documented blind spot


# ---------------------------------------------------------------------------
# 5. Swapped lat/lng detection — silent migration error
#
# Convention: PointType expects (lng, lat). A common migration bug passes
# (lat, lng) instead. If lat is in [-90,90] and lng is in (90,180], the
# swap is undetectable — both orderings are geometrically valid numbers.
# But if the true longitude exceeds 90 (i.e. most of Asia, Pacific, Australia)
# the swapped value lands in lat position outside [-90,90] and raises.
#
# We also add an explicit warning for the ambiguous zone where both orderings
# are numerically valid — that case can only be caught at the app layer.
# ---------------------------------------------------------------------------

DETECTABLE_SWAPS = [
    # (wrongly_passed_lat, wrongly_passed_lng, desc)
    # These will raise because the true lng > 90, so when swapped into lat position it's out of range
    (40.7128,  -74.0060, "New York — lng -74 is valid lat, but swap detectable if user passes backwards"),
    (35.6895,  139.6917, "Tokyo — lng 139 in lat position raises immediately"),
    (-33.8688, 151.2093, "Sydney — lng 151 in lat position raises immediately"),
    (1.3521,   103.8198, "Singapore — lng 103 in lat position raises"),
    (64.1466,  -21.9426, "Reykjavik — detectable only if lng passed as lat exceeds 90"),
]

AMBIGUOUS_SWAPS = [
    # Both (lat, lng) and (lng, lat) are numerically valid — undetectable at type layer
    # lng is within [-90, 90] so it looks like a valid lat either way
    (40.7128,  -74.0060, "NYC lng -74 fits in lat range — ambiguous"),
    (51.5074,   -0.1278, "London — both values within [-90,90] — ambiguous"),
    (48.8566,    2.3522, "Paris — both values within [-90,90] — ambiguous"),
]


@pytest.mark.parametrize("lat,lng,desc", DETECTABLE_SWAPS)
def test_detectable_swap_raises(lat, lng, desc):
    """
    When a true longitude > 90 is accidentally passed in the lat position,
    _validate_point must raise ValueError. This catches the most dangerous
    class of swap — coordinates in Asia, Pacific, Australia, Americas > 90W.
    """
    if abs(lng) <= 90:
        pytest.skip(f"[{desc}] lng {lng} fits in lat range — ambiguous, not detectable at type layer")

    with pytest.raises(ValueError, match=r"(Longitude|Latitude) must be within"):
        bind((lat, lng))  # intentionally swapped: lat first, lng second


@pytest.mark.parametrize("lat,lng,desc", AMBIGUOUS_SWAPS)
def test_ambiguous_swap_is_silently_accepted(lat, lng, desc):
    """
    Documents the known limitation: when both lat and lng fall within [-90,90],
    a swap is geometrically undetectable at the type layer. This test passes
    (no error raised) to make the blind spot explicit in the test suite.
    The fix belongs in application-layer validation, not PointType.
    """
    # Both orderings are valid numbers — PointType cannot know which is correct
    result_correct = bind((lng, lat))   # correct:  (lng, lat)
    result_swapped = bind((lat, lng))   # swapped:  (lat, lng) — silently accepted

    assert result_correct is not None
    assert result_swapped is not None  # this is the documented blind spot


# ---------------------------------------------------------------------------
# 5. Swapped lat/lng detection — silent migration error
#
# Convention: PointType expects (lng, lat). A common migration bug passes
# (lat, lng) instead. If lat is in [-90,90] and lng is in (90,180], the
# swap is undetectable — both orderings are geometrically valid numbers.
# But if the true longitude exceeds 90 (i.e. most of Asia, Pacific, Australia)
# the swapped value lands in lat position outside [-90,90] and raises.
#
# We also add an explicit warning for the ambiguous zone where both orderings
# are numerically valid — that case can only be caught at the app layer.
# ---------------------------------------------------------------------------

DETECTABLE_SWAPS = [
    # (wrongly_passed_lat, wrongly_passed_lng, desc)
    # These will raise because the true lng > 90, so when swapped into lat position it's out of range
    (40.7128,  -74.0060, "New York — lng -74 is valid lat, but swap detectable if user passes backwards"),
    (35.6895,  139.6917, "Tokyo — lng 139 in lat position raises immediately"),
    (-33.8688, 151.2093, "Sydney — lng 151 in lat position raises immediately"),
    (1.3521,   103.8198, "Singapore — lng 103 in lat position raises"),
    (64.1466,  -21.9426, "Reykjavik — detectable only if lng passed as lat exceeds 90"),
]

AMBIGUOUS_SWAPS = [
    # Both (lat, lng) and (lng, lat) are numerically valid — undetectable at type layer
    # lng is within [-90, 90] so it looks like a valid lat either way
    (40.7128,  -74.0060, "NYC lng -74 fits in lat range — ambiguous"),
    (51.5074,   -0.1278, "London — both values within [-90,90] — ambiguous"),
    (48.8566,    2.3522, "Paris — both values within [-90,90] — ambiguous"),
]


@pytest.mark.parametrize("lat,lng,desc", DETECTABLE_SWAPS)
def test_detectable_swap_raises(lat, lng, desc):
    """
    When a true longitude > 90 is accidentally passed in the lat position,
    _validate_point must raise ValueError. This catches the most dangerous
    class of swap — coordinates in Asia, Pacific, Australia, Americas > 90W.
    """
    if abs(lng) <= 90:
        pytest.skip(f"[{desc}] lng {lng} fits in lat range — ambiguous, not detectable at type layer")

    with pytest.raises(ValueError, match=r"(Longitude|Latitude) must be within"):
        bind((lat, lng))  # intentionally swapped: lat first, lng second


@pytest.mark.parametrize("lat,lng,desc", AMBIGUOUS_SWAPS)
def test_ambiguous_swap_is_silently_accepted(lat, lng, desc):
    """
    Documents the known limitation: when both lat and lng fall within [-90,90],
    a swap is geometrically undetectable at the type layer. This test passes
    (no error raised) to make the blind spot explicit in the test suite.
    The fix belongs in application-layer validation, not PointType.
    """
    # Both orderings are valid numbers — PointType cannot know which is correct
    result_correct = bind((lng, lat))   # correct:  (lng, lat)
    result_swapped = bind((lat, lng))   # swapped:  (lat, lng) — silently accepted

    assert result_correct is not None
    assert result_swapped is not None  # this is the documented blind spot


# ---------------------------------------------------------------------------
# 5. Swapped lat/lng detection — silent migration error
#
# Convention: PointType expects (lng, lat). A common migration bug passes
# (lat, lng) instead. If lat is in [-90,90] and lng is in (90,180], the
# swap is undetectable — both orderings are valid numbers. But if the true
# longitude exceeds 90, the swapped value lands out of lat range and raises.
# ---------------------------------------------------------------------------

DETECTABLE_SWAPS = [
    (35.6895,  139.6917, "Tokyo — lng 139 in lat position raises"),
    (-33.8688, 151.2093, "Sydney — lng 151 in lat position raises"),
    (1.3521,   103.8198, "Singapore — lng 103 in lat position raises"),
]

AMBIGUOUS_SWAPS = [
    (51.5074,  -0.1278, "London — both values within [-90,90] — undetectable"),
    (48.8566,   2.3522, "Paris — both values within [-90,90] — undetectable"),
]


@pytest.mark.parametrize("lat,lng,desc", DETECTABLE_SWAPS)
def test_detectable_swap_raises(lat, lng, desc):
    """Lng > 90 in lat position must raise — catches Asia/Pacific/Australia swaps."""
    with pytest.raises(ValueError, match=r"(Longitude|Latitude) must be within"):
        bind((lat, lng))  # intentionally swapped


@pytest.mark.parametrize("lat,lng,desc", AMBIGUOUS_SWAPS)
def test_ambiguous_swap_is_silently_accepted(lat, lng, desc):
    """Documents the blind spot: when both values fit in [-90,90], PointType
    cannot detect a swap. Fix belongs in app-layer validation, not PointType."""
    assert bind((lng, lat)) is not None  # correct
    assert bind((lat, lng)) is not None  # swapped — silently accepted, documented blind spot
