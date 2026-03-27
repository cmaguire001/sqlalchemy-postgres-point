"""
infer.py

Batch axis-order inference for ambiguous GPS coordinate pairs.
Implements infer_axis_order() against the contract in INFER_GUARANTEE.md.

Public API:
    infer_axis_order(points, timestamps=None, region_hint=None, threshold=0.85)

See INFER_GUARANTEE.md for formal guarantees, failure mode classification,
and domain-specific reliability notes.
"""

import math
import statistics
from typing import Any, Iterable, Optional


# ── constants ─────────────────────────────────────────────────────────────────

MINIMUM_SAMPLE_SIZE = 30

REGION_HINTS = {
    "europe":          {"lon": (  -25.0,  45.0), "lat": ( 34.0,  72.0)},
    "north_america":   {"lon": (-168.0,  -52.0), "lat": ( 15.0,  72.0)},
    "south_america":   {"lon": ( -82.0,  -34.0), "lat": (-56.0,  13.0)},
    "asia":            {"lon": (  26.0,  145.0), "lat": ( -10.0, 77.0)},
    "africa":          {"lon": ( -18.0,   52.0), "lat": ( -35.0, 38.0)},
    "oceania":         {"lon": ( 112.0,  180.0), "lat": ( -47.0,  -8.0)},
    "maritime_pacific":{"lon": (-180.0,  180.0), "lat": ( -60.0,  60.0)},
    "arctic":          {"lon": (-180.0,  180.0), "lat": (  75.0,  90.0)},
    "antarctic":       {"lon": (-180.0,  180.0), "lat": ( -90.0, -75.0)},
}


# ── internal helpers ──────────────────────────────────────────────────────────

def _std(values):
    """Population std dev. Returns 0.0 for fewer than 2 values."""
    if len(values) < 2:
        return 0.0
    mean = sum(values) / len(values)
    variance = sum((v - mean) ** 2 for v in values) / len(values)
    return math.sqrt(variance)


def _unwrap_longitude(lons):
    """
    Unwrap longitude sequence to remove antimeridian discontinuities.
    Consecutive jumps > 180 degrees are assumed to be antimeridian crossings.
    Returns (unwrapped_lons, crossing_detected).
    """
    if len(lons) < 2:
        return list(lons), False

    unwrapped = [lons[0]]
    crossing = False
    for i in range(1, len(lons)):
        delta = lons[i] - lons[i - 1]
        if delta > 180.0:
            delta -= 360.0
            crossing = True
        elif delta < -180.0:
            delta += 360.0
            crossing = True
        unwrapped.append(unwrapped[-1] + delta)
    return unwrapped, crossing


def _hemisphere_consistency(values, negative_bound, positive_bound):
    """
    What fraction of values share the same sign hemisphere?
    Returns 0.5 for perfectly mixed, 1.0 for perfectly consistent.
    """
    if not values:
        return 0.5
    positive = sum(1 for v in values if v >= 0)
    negative = len(values) - positive
    dominant = max(positive, negative)
    return dominant / len(values)


def _in_region(a_vals, b_vals, region):
    """
    What fraction of (a, b) pairs fall inside region bbox
    when interpreted as (lon, lat)?
    """
    lon_min, lon_max = region["lon"]
    lat_min, lat_max = region["lat"]
    if not a_vals:
        return 0.0
    inside = sum(
        1 for a, b in zip(a_vals, b_vals)
        if lon_min <= a <= lon_max and lat_min <= b <= lat_max
    )
    return inside / len(a_vals)


def _bimodality_coefficient(values):
    """
    Sarle's bimodality coefficient. Values > 0.555 suggest bimodality.
    Requires at least 3 values.
    Reference: SAS Institute, 1990.
    """
    n = len(values)
    if n < 3:
        return 0.0
    mean = sum(values) / n
    diffs = [v - mean for v in values]
    m2 = sum(d ** 2 for d in diffs) / n
    m3 = sum(d ** 3 for d in diffs) / n
    m4 = sum(d ** 4 for d in diffs) / n
    if m2 == 0:
        return 0.0
    skewness = m3 / (m2 ** 1.5) if m2 > 0 else 0.0
    kurtosis = (m4 / (m2 ** 2)) - 3.0 if m2 > 0 else 0.0
    bc = (skewness ** 2 + 1.0) / (kurtosis + 3.0 * ((n - 1) ** 2) / ((n - 2) * (n - 3)))
    return round(bc, 4)


def _signal_result(vote, confidence, preconditions_met, note=None):
    r = {
        "vote":               vote,
        "confidence":         round(confidence, 4),
        "preconditions_met":  preconditions_met,
    }
    if note:
        r["note"] = note
    return r


# ── signal extractors ─────────────────────────────────────────────────────────

def _signal_spread_ratio(a_vals, b_vals, near_pole):
    """
    Longitude naturally has more spread than latitude for most datasets.
    std(lon) > std(lat) is weak evidence for (a=lon, b=lat).
    Disabled near poles where this assumption breaks down.
    """
    if near_pole:
        return _signal_result("abstain", 0.0, False,
                               "spread ratio unreliable near poles")
    if len(a_vals) < 2:
        return _signal_result("abstain", 0.0, False, "insufficient sample")

    std_a = _std(a_vals)
    std_b = _std(b_vals)

    if std_a == 0 and std_b == 0:
        return _signal_result("abstain", 0.0, False, "zero spread in both axes")

    if std_a == 0 or std_b == 0:
        return _signal_result("abstain", 0.0, False, "zero spread in one axis")

    ratio = std_a / std_b
    if ratio > 1.2:
        # a has more spread -- consistent with a=lon
        confidence = min(0.7, 0.3 + (ratio - 1.0) * 0.15)
        return _signal_result("lon_lat", confidence, True)
    elif ratio < 0.83:
        # b has more spread -- consistent with a=lat
        confidence = min(0.7, 0.3 + (1.0 / ratio - 1.0) * 0.15)
        return _signal_result("lat_lon", confidence, True)
    else:
        return _signal_result("abstain", 0.0, True, "spread ratio too close to call")


def _signal_hemisphere_coherence(a_vals, b_vals):
    """
    Real batches from one source share a hemisphere.
    Checks whether (a=lon, b=lat) or (a=lat, b=lon) produces
    more internally consistent hemispheres.
    """
    if len(a_vals) < 5:
        return _signal_result("abstain", 0.0, False, "insufficient sample")

    # Longitude range is [-180, 180], latitude [-90, 90]
    # A value outside [-90, 90] can only be longitude
    a_outside_lat = sum(1 for v in a_vals if abs(v) > 90)
    b_outside_lat = sum(1 for v in b_vals if abs(v) > 90)

    if a_outside_lat > 0 and b_outside_lat == 0:
        # a must be longitude -- only longitudes exceed ±90
        confidence = min(0.95, 0.6 + (a_outside_lat / len(a_vals)) * 0.35)
        return _signal_result("lon_lat", confidence, True,
                               "{} values exceed ±90 in axis a".format(a_outside_lat))
    elif b_outside_lat > 0 and a_outside_lat == 0:
        confidence = min(0.95, 0.6 + (b_outside_lat / len(b_vals)) * 0.35)
        return _signal_result("lat_lon", confidence, True,
                               "{} values exceed ±90 in axis b".format(b_outside_lat))
    elif a_outside_lat > 0 and b_outside_lat > 0:
        return _signal_result("abstain", 0.0, False,
                               "both axes contain values outside ±90 -- data may be invalid")

    # Both axes entirely within [-90, 90] -- ambiguous territory
    # Fall back to hemisphere consistency as a weak signal
    lon_lat_score = (
        _hemisphere_consistency(a_vals, -180, 180) +
        _hemisphere_consistency(b_vals, -90, 90)
    ) / 2.0

    lat_lon_score = (
        _hemisphere_consistency(b_vals, -180, 180) +
        _hemisphere_consistency(a_vals, -90, 90)
    ) / 2.0

    diff = abs(lon_lat_score - lat_lon_score)
    if diff < 0.05:
        return _signal_result("abstain", 0.0, True, "hemisphere scores too close")

    if lon_lat_score > lat_lon_score:
        return _signal_result("lon_lat", min(0.55, diff * 2), True)
    else:
        return _signal_result("lat_lon", min(0.55, diff * 2), True)


def _signal_trajectory_coherence(a_vals, b_vals, timestamps):
    """
    Consecutive GPS points should be geographically close relative to time delta.
    A swap produces impossible velocity spikes in one orientation but not the other.
    Requires timestamps. Antimeridian crossings are handled before this signal runs.
    """
    if timestamps is None:
        return _signal_result("abstain", 0.0, False, "no timestamps provided")
    if len(a_vals) < 3:
        return _signal_result("abstain", 0.0, False, "insufficient sample")
    if len(timestamps) != len(a_vals):
        return _signal_result("abstain", 0.0, False,
                               "timestamp count does not match point count")

    def velocity_variance(xs, ys, ts):
        speeds = []
        for i in range(1, len(xs)):
            dt = ts[i] - ts[i - 1]
            if dt <= 0:
                continue
            dx = xs[i] - xs[i - 1]
            dy = ys[i] - ys[i - 1]
            dist = math.sqrt(dx ** 2 + dy ** 2)
            speeds.append(dist / dt)
        return _std(speeds) if speeds else float("inf")

    var_as_given  = velocity_variance(a_vals, b_vals, timestamps)
    var_as_swapped = velocity_variance(b_vals, a_vals, timestamps)

    if var_as_given == float("inf") or var_as_swapped == float("inf"):
        return _signal_result("abstain", 0.0, False, "could not compute velocity")

    if var_as_given == 0 and var_as_swapped == 0:
        return _signal_result("abstain", 0.0, True, "stationary points -- no trajectory signal")

    total = var_as_given + var_as_swapped
    if total == 0:
        return _signal_result("abstain", 0.0, True, "zero total variance")

    ratio = var_as_swapped / total  # higher = swapped is worse = as_given is correct
    if ratio > 0.65:
        confidence = min(0.9, (ratio - 0.5) * 3.0)
        return _signal_result("lon_lat", confidence, True)
    elif ratio < 0.35:
        confidence = min(0.9, (0.5 - ratio) * 3.0)
        return _signal_result("lat_lon", confidence, True)
    else:
        return _signal_result("abstain", 0.0, True, "velocity variance too similar")


def _signal_region_hint(a_vals, b_vals, region_hint):
    """
    If caller provides a region hint, check what fraction of points
    land inside the bbox in each orientation.
    This is the strongest signal when available -- treat it as near-definitive.
    """
    if region_hint is None:
        return _signal_result("abstain", 0.0, False, "no region hint provided")

    region_key = region_hint.lower().replace(" ", "_")
    if region_key not in REGION_HINTS:
        return _signal_result("abstain", 0.0, False,
                               "unknown region hint: {}".format(region_hint))

    region = REGION_HINTS[region_key]
    frac_as_given   = _in_region(a_vals, b_vals, region)
    frac_as_swapped = _in_region(b_vals, a_vals, region)

    if frac_as_given > frac_as_swapped + 0.1:
        confidence = min(0.95, 0.6 + frac_as_given * 0.35)
        return _signal_result("lon_lat", confidence, True,
                               "{:.0f}% in region as given vs {:.0f}% swapped".format(
                                   frac_as_given * 100, frac_as_swapped * 100))
    elif frac_as_swapped > frac_as_given + 0.1:
        confidence = min(0.95, 0.6 + frac_as_swapped * 0.35)
        return _signal_result("lat_lon", confidence, True,
                               "{:.0f}% in region as swapped vs {:.0f}% as given".format(
                                   frac_as_swapped * 100, frac_as_given * 100))
    else:
        return _signal_result("abstain", 0.0, True,
                               "region check inconclusive -- similar fractions both ways")


# ── evidence aggregator ───────────────────────────────────────────────────────

SIGNAL_WEIGHTS = {
    "region_hint":           0.40,
    "hemisphere_coherence":  0.30,
    "trajectory_coherence":  0.20,
    "spread_ratio":          0.10,
}

def _aggregate(signals):
    """
    Weighted vote aggregation.
    Returns (probability_lon_lat, contradicting).
    Contradiction: active signals disagree on direction.
    """
    lon_lat_score = 0.0
    lat_lon_score = 0.0
    active_votes = set()

    for name, result in signals.items():
        if result["vote"] == "abstain" or not result["preconditions_met"]:
            continue
        weight = SIGNAL_WEIGHTS.get(name, 0.1)
        weighted = weight * result["confidence"]
        if result["vote"] == "lon_lat":
            lon_lat_score += weighted
            active_votes.add("lon_lat")
        elif result["vote"] == "lat_lon":
            lat_lon_score += weighted
            active_votes.add("lat_lon")

    total = lon_lat_score + lat_lon_score
    if total == 0:
        return 0.5, False  # no active signals -- pure uncertainty

    probability = lon_lat_score / total
    contradicting = len(active_votes) > 1  # signals disagree
    return probability, contradicting


# ── public API ────────────────────────────────────────────────────────────────

def infer_axis_order(
    points: Iterable[Any],
    timestamps: Optional[Iterable[float]] = None,
    region_hint: Optional[str] = None,
    threshold: float = 0.85,
) -> dict[str, Any]:
    """
    Infer whether coordinate pairs are in (lon, lat) or (lat, lon) order.

    Parameters
    ----------
    points      : Iterable of (a, b) pairs. Values must be numeric.
    timestamps  : Optional ordered sequence of numeric timestamps (epoch seconds
                  or any consistent unit). Enables trajectory coherence signal.
    region_hint : Optional string. One of: europe, north_america, south_america,
                  asia, africa, oceania, maritime_pacific, arctic, antarctic.
    threshold   : Confidence threshold for a definitive recommendation.
                  Default 0.85. Lower values increase Type I error risk.

    Returns
    -------
    dict with keys: recommended_order, probability, decision_threshold_used,
                    signals, data_quality, recommendation, warning.

    See INFER_GUARANTEE.md for formal guarantees and failure mode classification.
    """
    rows = list(points)
    ts   = list(timestamps) if timestamps is not None else None

    # ── parse input ───────────────────────────────────────────────────────────
    a_vals, b_vals = [], []
    parse_failures = 0
    for item in rows:
        try:
            a, b = item
            a_vals.append(float(a))
            b_vals.append(float(b))
        except Exception:
            parse_failures += 1

    n = len(a_vals)

    # ── data quality checks ───────────────────────────────────────────────────
    sample_size_adequate = n >= MINIMUM_SAMPLE_SIZE

    # Antimeridian unwrapping on both axes before any signal runs
    a_unwrapped, a_cross = _unwrap_longitude(a_vals)
    b_unwrapped, b_cross = _unwrap_longitude(b_vals)
    antimeridian = a_cross or b_cross

    # Use unwrapped values for trajectory signal only
    # Range-based signals use original values
    near_pole = any(abs(v) > 75.0 for v in a_vals + b_vals)

    # Bimodality check on each axis
    bc_a = _bimodality_coefficient(a_vals)
    bc_b = _bimodality_coefficient(b_vals)
    likely_mixed = bc_a > 0.555 or bc_b > 0.555

    data_quality = {
        "sample_size":                    n,
        "parse_failures":                 parse_failures,
        "sample_size_adequate":           sample_size_adequate,
        "near_pole":                      near_pole,
        "antimeridian_crossing_detected": antimeridian,
        "likely_mixed_sources":           likely_mixed,
        "bimodality_coefficient_a":       bc_a,
        "bimodality_coefficient_b":       bc_b,
        "signals_contradicting":          False,  # filled after aggregation
    }

    # ── early exit: empty or unparseable ─────────────────────────────────────
    if n == 0:
        return {
            "recommended_order":       "uncertain",
            "probability":             0.5,
            "decision_threshold_used": threshold,
            "signals":                 {},
            "data_quality":            data_quality,
            "recommendation":          "do_not_reorder",
            "warning":                 "no parseable points provided",
        }

    # ── run signals ───────────────────────────────────────────────────────────
    signals = {
        "spread_ratio":         _signal_spread_ratio(a_vals, b_vals, near_pole),
        "hemisphere_coherence": _signal_hemisphere_coherence(a_vals, b_vals),
        "trajectory_coherence": _signal_trajectory_coherence(
                                    a_unwrapped, b_unwrapped, ts),
        "region_hint":          _signal_region_hint(a_vals, b_vals, region_hint),
    }

    # ── aggregate ─────────────────────────────────────────────────────────────
    probability, contradicting = _aggregate(signals)
    data_quality["signals_contradicting"] = contradicting

    # ── decision ──────────────────────────────────────────────────────────────
    # G2: contradiction blocks a definitive recommendation regardless of probability
    # G4: uncertain is a valid complete answer
    if contradicting:
        recommended_order = "uncertain"
        recommendation    = "do_not_reorder"
        warning = ("Signals disagree on axis order. "
                   "Batch may contain mixed-source data. "
                   "Split by source and re-run before acting.")
    elif not sample_size_adequate:
        recommended_order = "uncertain"
        recommendation    = "flag_for_review"
        warning = ("Sample size {} is below minimum {}. "
                   "Results are statistically unreliable.").format(n, MINIMUM_SAMPLE_SIZE)
    elif likely_mixed:
        recommended_order = "uncertain"
        recommendation    = "do_not_reorder"
        warning = ("Bimodal distribution detected. "
                   "Batch likely contains data from multiple sources. "
                   "Split and re-run.")
    elif probability >= threshold:
        recommended_order = "lon_lat"
        recommendation    = "safe_to_reorder" if probability >= threshold else "flag_for_review"
        warning = None
    elif (1.0 - probability) >= threshold:
        recommended_order = "lat_lon"
        recommendation    = "safe_to_reorder"
        warning = None
    else:
        recommended_order = "uncertain"
        recommendation    = "flag_for_review"
        warning = ("Probability {:.2f} did not meet threshold {:.2f}. "
                   "Axis order cannot be determined confidently.").format(probability, threshold)

    return {
        "recommended_order":       recommended_order,
        "probability":             round(probability, 4),
        "decision_threshold_used": threshold,
        "signals":                 signals,
        "data_quality":            data_quality,
        "recommendation":          recommendation,
        "warning":                 warning,
    }
