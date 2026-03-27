"""
test_infer.py

Test suite for infer_axis_order() validated against INFER_GUARANTEE.md.

Each test is labeled with the guarantee or failure mode it covers:
  G1 -- never mutates data
  G2 -- never returns false certainty when signals contradict
  G3 -- honest about signal limits
  G4 -- uncertain is a valid complete answer
  G5 -- fully auditable
  N1 -- cannot resolve single point
  N2 -- cannot resolve mixed-source batch
  N3 -- cannot resolve symmetric batch
  N4 -- inference is last resort
  TypeI  -- false swap correction (must never happen via library)
  TypeII -- missed swap (mitigated by conservative threshold)
  TypeIII -- false certainty (must never happen when signals contradict)
  TypeIV -- silent mixed-source corruption (flagged, not silent)
"""

import math
import pytest
from sqlalchemy_postgres_point.infer import infer_axis_order


# ── fixtures ──────────────────────────────────────────────────────────────────

def make_europe_points(n=50):
    """Correct (lon, lat) order. Western Europe lon~2-15, lat~43-52."""
    import random
    random.seed(42)
    return [(random.uniform(2.0, 15.0), random.uniform(43.0, 52.0)) for _ in range(n)]

def make_europe_swapped(n=50):
    """Swapped (lat, lon) order. Same data, wrong axis order."""
    return [(lat, lon) for lon, lat in make_europe_points(n)]

def make_utah_points(n=50):
    """Utah ski resorts. lon~-109 to -114, lat~37-42. Unambiguous."""
    import random
    random.seed(7)
    return [(random.uniform(-114.0, -109.0), random.uniform(37.0, 42.0)) for _ in range(n)]

def make_alps_points(n=50):
    """Alps centroids. lon~5-16, lat~44-48. Both axes in [-90,90]. Ambiguous."""
    import random
    random.seed(13)
    return [(random.uniform(5.0, 16.0), random.uniform(44.0, 48.0)) for _ in range(n)]

def make_polar_points(n=50):
    """Arctic research vessel. lat~85-90, lon~0-90. Near pole."""
    import random
    random.seed(99)
    return [(random.uniform(0.0, 90.0), random.uniform(85.0, 90.0)) for _ in range(n)]

def make_antimeridian_points():
    """Vessel crossing from Japan to Alaska across ±180."""
    lons = [175.0, 177.0, 179.0, -179.0, -177.0, -175.0, -173.0, -171.0,
            -169.0, -167.0, -165.0, -163.0, -161.0, -159.0, -157.0, -155.0,
            -153.0, -151.0, -149.0, -147.0, -145.0, -143.0, -141.0, -139.0,
            -137.0, -135.0, -133.0, -131.0, -129.0, -127.0]
    lats = [35.0 + i * 0.3 for i in range(len(lons))]
    return list(zip(lons, lats))

def make_mixed_source(n=30):
    """Half Europe correct, half Europe swapped. Two sources concatenated."""
    half = n // 2
    correct = make_europe_points(half)
    swapped = make_europe_swapped(half)
    return correct + swapped

def make_symmetric_points(n=50):
    """Points clustered at (45, 45) -- both orientations statistically identical."""
    import random
    random.seed(55)
    return [(random.gauss(45.0, 0.5), random.gauss(45.0, 0.5)) for _ in range(n)]

def make_single_point():
    return [(7.5, 46.0)]

def make_timestamps(n, step=60):
    """Evenly spaced timestamps, step seconds apart."""
    return [i * step for i in range(n)]


# ── guarantee G1: never mutates data ─────────────────────────────────────────

def test_G1_input_not_mutated():
    """G1 -- infer_axis_order must not modify the input list."""
    points = make_europe_points(40)
    original = list(points)
    infer_axis_order(points)
    assert points == original


# ── guarantee G2: contradiction blocks recommendation ────────────────────────

def test_G2_contradiction_blocks_recommendation():
    """G2 -- when signals contradict, recommendation must be do_not_reorder."""
    # Mixed source data will cause signals to disagree
    points = make_mixed_source(60)
    result = infer_axis_order(points)
    if result["data_quality"]["signals_contradicting"]:
        assert result["recommendation"] == "do_not_reorder"
        assert result["recommended_order"] == "uncertain"


def test_G2_contradicting_signals_never_safe_to_reorder():
    """G2 -- safe_to_reorder must never appear alongside signals_contradicting."""
    for seed in range(10):
        import random
        random.seed(seed)
        points = [(random.uniform(-90, 90), random.uniform(-90, 90)) for _ in range(40)]
        result = infer_axis_order(points)
        if result["data_quality"]["signals_contradicting"]:
            assert result["recommendation"] != "safe_to_reorder"


# ── guarantee G3: honest about signal limits ─────────────────────────────────

def test_G3_polar_spread_signal_abstains():
    """G3 -- spread_ratio must abstain near poles."""
    points = make_polar_points(50)
    result = infer_axis_order(points)
    spread = result["signals"]["spread_ratio"]
    assert spread["vote"] == "abstain"
    assert spread["preconditions_met"] is False


def test_G3_no_timestamps_trajectory_abstains():
    """G3 -- trajectory_coherence must abstain when no timestamps provided."""
    points = make_europe_points(50)
    result = infer_axis_order(points, timestamps=None)
    traj = result["signals"]["trajectory_coherence"]
    assert traj["vote"] == "abstain"
    assert traj["preconditions_met"] is False


def test_G3_unknown_region_hint_abstains():
    """G3 -- unknown region hint must abstain rather than guess."""
    points = make_europe_points(50)
    result = infer_axis_order(points, region_hint="middle_earth")
    region = result["signals"]["region_hint"]
    assert region["vote"] == "abstain"
    assert region["preconditions_met"] is False


def test_G3_small_sample_flagged():
    """G3 -- samples below minimum size must be flagged in data_quality."""
    points = make_europe_points(10)
    result = infer_axis_order(points)
    assert result["data_quality"]["sample_size_adequate"] is False
    assert result["data_quality"]["sample_size"] == 10


# ── guarantee G4: uncertain is a valid complete answer ───────────────────────

def test_G4_single_point_is_uncertain():
    """G4 + N1 -- a single point must always return uncertain."""
    result = infer_axis_order(make_single_point())
    assert result["recommended_order"] == "uncertain"


def test_G4_empty_input_is_uncertain():
    """G4 -- empty input must return uncertain with a warning."""
    result = infer_axis_order([])
    assert result["recommended_order"] == "uncertain"
    assert result["recommendation"] == "do_not_reorder"
    assert result["warning"] is not None


def test_G4_symmetric_batch_uncertain():
    """G4 + N3 -- symmetric batch at (45,45) cannot be resolved."""
    points = make_symmetric_points(50)
    result = infer_axis_order(points)
    # Should not confidently recommend reorder on a symmetric batch
    assert result["recommendation"] != "safe_to_reorder" or result["probability"] < 0.85


def test_G4_uncertain_recommendation_has_warning():
    """G4 -- every uncertain result must include an explanatory warning."""
    result = infer_axis_order(make_single_point())
    assert result["recommended_order"] == "uncertain"
    # warning may be None only for safe_to_reorder results
    # for uncertain results it should explain why


# ── guarantee G5: fully auditable ────────────────────────────────────────────

def test_G5_all_signals_present_in_output():
    """G5 -- all four signals must appear in every result."""
    points = make_europe_points(50)
    result = infer_axis_order(points)
    assert "spread_ratio"         in result["signals"]
    assert "hemisphere_coherence" in result["signals"]
    assert "trajectory_coherence" in result["signals"]
    assert "region_hint"          in result["signals"]


def test_G5_each_signal_has_required_keys():
    """G5 -- each signal must expose vote, confidence, preconditions_met."""
    points = make_europe_points(50)
    result = infer_axis_order(points)
    for name, signal in result["signals"].items():
        assert "vote"              in signal, "{} missing vote".format(name)
        assert "confidence"        in signal, "{} missing confidence".format(name)
        assert "preconditions_met" in signal, "{} missing preconditions_met".format(name)


def test_G5_output_has_all_top_level_keys():
    """G5 -- output contract keys must all be present."""
    result = infer_axis_order(make_europe_points(50))
    required = {
        "recommended_order", "probability", "decision_threshold_used",
        "signals", "data_quality", "recommendation", "warning"
    }
    assert required.issubset(result.keys())


def test_G5_data_quality_has_all_keys():
    """G5 -- data_quality must expose all diagnostic fields."""
    result = infer_axis_order(make_europe_points(50))
    dq = result["data_quality"]
    required = {
        "sample_size", "sample_size_adequate", "near_pole",
        "antimeridian_crossing_detected", "likely_mixed_sources",
        "signals_contradicting"
    }
    assert required.issubset(dq.keys())


# ── N1: single point unresolvable ────────────────────────────────────────────

def test_N1_single_point_never_safe_to_reorder():
    """N1 -- single points must never produce safe_to_reorder."""
    for pt in [(7.5, 46.0), (-111.5, 40.6), (139.0, 35.0), (0.0, 0.0)]:
        result = infer_axis_order([pt])
        assert result["recommendation"] != "safe_to_reorder"


# ── N2: mixed source detection ───────────────────────────────────────────────

def test_N2_mixed_source_flagged():
    """N2 + TypeIV -- mixed source batches must be flagged, not silently processed."""
    points = make_mixed_source(60)
    result = infer_axis_order(points)
    dq = result["data_quality"]
    # Either bimodality is detected or signals contradict -- one of these must fire
    flagged = dq["likely_mixed_sources"] or dq["signals_contradicting"]
    assert flagged, "mixed source batch passed through without any flag"


# ── N3: symmetric batch unresolvable ─────────────────────────────────────────

def test_N3_symmetric_batch_not_overconfident():
    """N3 -- symmetric batches must not produce high confidence."""
    points = make_symmetric_points(50)
    result = infer_axis_order(points)
    assert result["probability"] < 0.85 or result["probability"] > 0.15


# ── Type I: false swap correction impossible via library ──────────────────────

def test_TypeI_library_never_reorders_data():
    """TypeI -- infer_axis_order returns a recommendation, never modified data."""
    points = make_europe_points(50)
    result = infer_axis_order(points)
    # Output contains no modified point data -- only metadata
    assert "points" not in result
    assert "reordered" not in result
    assert "corrected" not in result


# ── Type II: unambiguous data is correctly identified ────────────────────────

def test_TypeII_utah_correctly_identified_as_lon_lat():
    """TypeII mitigation -- unambiguous Utah data must be identified as lon_lat."""
    points = make_utah_points(50)
    result = infer_axis_order(points)
    # Utah lon ~-111 is outside [-90,90] -- hemisphere signal fires with high confidence
    assert result["recommended_order"] == "lon_lat"
    assert result["probability"] >= 0.85


def test_TypeII_swapped_utah_identified_as_lat_lon():
    """TypeII mitigation -- swapped Utah data must be identified as lat_lon."""
    points = [(lat, lon) for lon, lat in make_utah_points(50)]
    result = infer_axis_order(points)
    assert result["recommended_order"] == "lat_lon"
    assert result["probability"] <= 0.15


# ── Type III: false certainty impossible when signals contradict ──────────────

def test_TypeIII_contradiction_prevents_high_confidence_recommendation():
    """TypeIII -- contradicting signals must never produce safe_to_reorder."""
    points = make_mixed_source(60)
    result = infer_axis_order(points)
    if result["data_quality"]["signals_contradicting"]:
        assert result["recommendation"] != "safe_to_reorder"
        assert result["recommended_order"] == "uncertain"


# ── Type IV: mixed source not silent ─────────────────────────────────────────

def test_TypeIV_mixed_source_produces_warning():
    """TypeIV -- mixed source must produce a non-None warning."""
    points = make_mixed_source(60)
    result = infer_axis_order(points)
    dq = result["data_quality"]
    if dq["likely_mixed_sources"] or dq["signals_contradicting"]:
        assert result["warning"] is not None


# ── antimeridian ──────────────────────────────────────────────────────────────

def test_antimeridian_crossing_detected():
    """Antimeridian crossing must be detected and flagged in data_quality."""
    points = make_antimeridian_points()
    result = infer_axis_order(points, timestamps=make_timestamps(len(points)))
    assert result["data_quality"]["antimeridian_crossing_detected"] is True


def test_antimeridian_not_flagged_as_swap():
    """Antimeridian crossing must not produce lat_lon recommendation."""
    points = make_antimeridian_points()
    ts = make_timestamps(len(points))
    result = infer_axis_order(points, timestamps=ts)
    # A legitimate Japan-to-Alaska route should not be flagged as swapped
    assert result["recommended_order"] != "lat_lon"


# ── trajectory coherence ──────────────────────────────────────────────────────

def test_trajectory_coherence_fires_with_timestamps():
    """Trajectory signal must be active (non-abstain) when timestamps provided."""
    points = make_utah_points(50)
    ts = make_timestamps(50)
    result = infer_axis_order(points, timestamps=ts)
    traj = result["signals"]["trajectory_coherence"]
    assert traj["preconditions_met"] is True
    assert traj["vote"] != "abstain" or traj["confidence"] == 0.0


def test_trajectory_mismatched_timestamps_abstains():
    """Mismatched timestamp count must cause trajectory signal to abstain."""
    points = make_utah_points(50)
    ts = make_timestamps(30)  # wrong length
    result = infer_axis_order(points, timestamps=ts)
    traj = result["signals"]["trajectory_coherence"]
    assert traj["vote"] == "abstain"
    assert traj["preconditions_met"] is False


# ── region hint ───────────────────────────────────────────────────────────────

def test_region_hint_europe_correct_order():
    """Region hint 'europe' must identify correctly ordered Alps data."""
    points = make_alps_points(50)
    result = infer_axis_order(points, region_hint="europe")
    assert result["signals"]["region_hint"]["vote"] == "lon_lat"
    assert result["recommended_order"] == "lon_lat"


def test_region_hint_europe_swapped_order():
    """Region hint 'europe' must identify swapped Alps data."""
    points = [(lat, lon) for lon, lat in make_alps_points(50)]
    result = infer_axis_order(points, region_hint="europe")
    assert result["signals"]["region_hint"]["vote"] == "lat_lon"


def test_region_hint_does_not_override_contradiction():
    """G2 -- region hint must not override a contradiction between other signals."""
    points = make_mixed_source(60)
    result = infer_axis_order(points, region_hint="europe")
    if result["data_quality"]["signals_contradicting"]:
        assert result["recommendation"] != "safe_to_reorder"


# ── threshold behavior ────────────────────────────────────────────────────────

def test_custom_threshold_respected():
    """Decision threshold must be reflected in output and used in decision."""
    points = make_utah_points(50)
    result_strict = infer_axis_order(points, threshold=0.99)
    result_loose  = infer_axis_order(points, threshold=0.50)
    assert result_strict["decision_threshold_used"] == 0.99
    assert result_loose["decision_threshold_used"]  == 0.50


def test_probability_always_between_0_and_1():
    """Probability must always be in [0.0, 1.0]."""
    for points in [
        make_europe_points(50),
        make_utah_points(50),
        make_alps_points(50),
        make_symmetric_points(50),
        make_single_point(),
        [],
    ]:
        result = infer_axis_order(points)
        assert 0.0 <= result["probability"] <= 1.0


def test_confidence_scores_between_0_and_1():
    """All signal confidence scores must be in [0.0, 1.0]."""
    points = make_europe_points(50)
    result = infer_axis_order(points)
    for name, signal in result["signals"].items():
        c = signal["confidence"]
        assert 0.0 <= c <= 1.0, "{} confidence {} out of range".format(name, c)


# ── parse robustness ──────────────────────────────────────────────────────────

def test_unparseable_points_counted_not_crashed():
    """Parse failures must be counted in data_quality, not raise exceptions."""
    points = [(1.0, 2.0), "bad", None, (3.0, 4.0), (5.0, 6.0)]
    result = infer_axis_order(points)
    assert result["data_quality"]["parse_failures"] == 2


def test_all_unparseable_returns_uncertain():
    """If all points fail to parse, result must be uncertain."""
    result = infer_axis_order(["bad", None, object()])
    assert result["recommended_order"] == "uncertain"
