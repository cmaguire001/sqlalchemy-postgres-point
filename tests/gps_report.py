"""
GPS Validation Report
Covers: earth_distance pairs, point serialization, coordinate validation
"""

from geopy.distance import great_circle

# ── 1. LOCATIONS ────────────────────────────────────────────────────────────
LOCATIONS = {
    "Park City UT":      (-111.4980,  40.6461),
    "Salt Lake City UT": (-111.8910,  40.7608),
    "New York City NY":  (-74.0060,   40.7128),
    "Los Angeles CA":    (-118.2437,  34.0522),
    "Chicago IL":        (-87.6298,   41.8781),
    "Miami FL":          (-80.1918,   25.7617),
    "London UK":         (-0.1278,    51.5074),
    "Tokyo JP":          (139.6917,   35.6895),
    "Sydney AU":         (151.2093,  -33.8688),
    "Reykjavik IS":      (-21.9426,   64.1466),
    "Singapore SG":      (103.8198,    1.3521),
    "Cape Town ZA":      (18.4241,   -33.9249),
    "Buenos Aires AR":   (-58.3816,  -34.6037),
    "Oslo NO":           (10.7522,    59.9139),
}

EXPECTED_DISTANCES = {
    ("Park City UT",     "Salt Lake City UT"): 22.06,
    ("New York City NY", "Los Angeles CA"):    2445.56,
    ("New York City NY", "London UK"):         3461.18,
    ("Los Angeles CA",   "Tokyo JP"):          5477.69,
    ("Miami FL",         "Reykjavik IS"):      3681.85,
    ("Singapore SG",     "Sydney AU"):         3918.53,
    ("Cape Town ZA",     "Reykjavik IS"):      7123.15,
    ("New York City NY", "Sydney AU"):         9934.97,
    ("Buenos Aires AR",  "Oslo NO"):           7610.37,
}

TOLERANCE_PCT = 0.01

# ── 2. POINT SERIALIZATION CASES ────────────────────────────────────────────
SERIALIZATION_CASES = [
    {"input": (1.23, 4.56), "expected_proc":  "(1.23,4.56)",  "label": "process_bind_param"},
    {"input": (1.23, 4.56), "expected_proc": "'(1.23,4.56)'", "label": "process_literal_param"},
    {"input": "(1.23,4.56)", "expected_proc":  (1.23, 4.56),  "label": "process_result_value"},
]

# ── 3. COORDINATE VALIDATION CASES ──────────────────────────────────────────
VALIDATION_CASES = [
    {"coord": (1.0,  2.0),   "note": "valid"},
    {"coord": (40.7, -74.0), "note": "valid NYC-like"},
    {"coord": (200.0, 50.0), "note": "invalid lon > 180"},
    {"coord": (40.7, 200.0), "note": "likely swapped (lat in lon slot)"},
]


def fmt(val, width=12):
    return str(val).ljust(width)


def section(title):
    print()
    print("=" * 70)
    print(f"  {title}")
    print("=" * 70)


# ── REPORT ───────────────────────────────────────────────────────────────────
print()
print("╔══════════════════════════════════════════════════════════════════╗")
print("║             GPS VALIDATION REPORT — sqlalchemy-postgres-point   ║")
print("╚══════════════════════════════════════════════════════════════════╝")

# Section 1 — Named coordinates
section("1. INPUT COORDINATES (14 named locations)")
print(f"  {'Location':<22} {'Longitude':>12} {'Latitude':>12}  {'Valid?':>7}")
print(f"  {'-'*22} {'-'*12} {'-'*12}  {'-'*7}")
for name, (lon, lat) in LOCATIONS.items():
    valid = -180 <= lon <= 180 and -90 <= lat <= 90
    flag = "✓" if valid else "✗ INVALID"
    print(f"  {name:<22} {lon:>12.4f} {lat:>12.4f}  {flag:>7}")

# Section 2 — Earth distance results
section("2. EARTH DISTANCE RESULTS (9 city pairs, 1% tolerance)")
print(f"  {'Pair':<42} {'Expected':>10} {'Actual':>10} {'Delta':>8} {'Result':>7}")
print(f"  {'-'*42} {'-'*10} {'-'*10} {'-'*8} {'-'*7}")

all_pass = True
for (city_a, city_b), expected in EXPECTED_DISTANCES.items():
    lon_a, lat_a = LOCATIONS[city_a]
    lon_b, lat_b = LOCATIONS[city_b]
    actual = great_circle((lat_a, lon_a), (lat_b, lon_b)).miles
    delta = abs(actual - expected)
    tol = expected * TOLERANCE_PCT
    passed = delta <= tol
    if not passed:
        all_pass = False
    label = f"{city_a} → {city_b}"
    status = "PASS ✓" if passed else "FAIL ✗"
    print(f"  {label:<42} {expected:>10.2f} {actual:>10.2f} {delta:>8.3f} {status:>7}")

print()
print(f"  Overall: {'ALL PASS ✓' if all_pass else 'FAILURES DETECTED ✗'}")

# Section 3 — Point serialization
section("3. POINT SERIALIZATION")
print(f"  {'Test':<26} {'Input':<18} {'Expected Output':<20} {'Status':>7}")
print(f"  {'-'*26} {'-'*18} {'-'*20} {'-'*7}")
for case in SERIALIZATION_CASES:
    print(f"  {case['label']:<26} {str(case['input']):<18} {str(case['expected_proc']):<20} {'(see tests)':>7}")

# Section 4 — Coordinate validation
section("4. COORDINATE VALIDATION (swap detection / invalid flagging)")
print(f"  {'Coordinate':<22} {'Note':<32} {'Lon valid?':>10} {'Lat valid?':>10}")
print(f"  {'-'*22} {'-'*32} {'-'*10} {'-'*10}")
for case in VALIDATION_CASES:
    lon, lat = case["coord"]
    lon_ok = "✓" if -180 <= lon <= 180 else "✗"
    lat_ok = "✓" if -90 <= lat <= 90 else "✗"
    print(f"  {str(case['coord']):<22} {case['note']:<32} {lon_ok:>10} {lat_ok:>10}")

print()
print("  Note: likely_swapped_pct and invalid_pct assertions validated in test_point.py")
print()
print("=" * 70)
print("  END OF REPORT")
print("=" * 70)
print()
