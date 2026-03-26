"""
audit_ski_points.py

Comprehensive data quality audit of ski_features.location using the full
sqlalchemy-postgres-point library surface:

  - analyze_point()      per-row confidence scoring + issue detection
  - validate_points()    batch summary metrics
  - PointType            bind_processor, result_processor, strict mode
  - earth_distance       comparator against a real origin point

Run:
    python audit_ski_points.py
"""

import os
import sys
import json
import warnings
from collections import defaultdict

import psycopg2
from dotenv import load_dotenv
from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.orm import DeclarativeBase, Session
from sqlalchemy_postgres_point import PointType, analyze_point, validate_points

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    print("ERROR: DATABASE_URL not set in .env")
    sys.exit(1)

PARK_CITY_LNG = -111.5080
PARK_CITY_LAT = 40.6461
PARK_CITY = (PARK_CITY_LNG, PARK_CITY_LAT)


class Base(DeclarativeBase):
    pass

class SkiFeature(Base):
    __tablename__ = "ski_features"
    id           = Column(Integer, primary_key=True)
    name         = Column(String)
    feature_type = Column(String)
    location     = Column(PointType())


def section(title):
    print("\n" + "=" * 58)
    print("  " + title)
    print("=" * 58)


def fetch_raw_rows(database_url):
    conn = psycopg2.connect(database_url)
    cur = conn.cursor()
    cur.execute("SELECT id, name, feature_type, location::text FROM ski_features ORDER BY id;")
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows


def parse_point_str(s):
    if s is None:
        return None
    s = s.strip().strip("()")
    parts = s.split(",")
    if len(parts) != 2:
        return None
    try:
        return (float(parts[0]), float(parts[1]))
    except ValueError:
        return None


def run_batch_validation(raw_rows):
    section("1 -- validate_points()  BATCH SUMMARY")
    points = []
    for _, _, _, loc_str in raw_rows:
        pt = parse_point_str(loc_str)
        if pt:
            points.append(pt)
    summary = validate_points(points)
    valid_pct = 100.0 - summary["likely_swapped_pct"] - summary["invalid_pct"] - summary["ambiguous_pct"]
    print("  Total rows:    {}".format(summary["total_rows"]))
    print("  Valid:         {:.2f}%".format(valid_pct))
    print("  Swapped:       {:.2f}%".format(summary["likely_swapped_pct"]))
    print("  Invalid:       {:.2f}%".format(summary["invalid_pct"]))
    print("  Ambiguous:     {:.2f}%".format(summary["ambiguous_pct"]))
    print("  Flagged rows:  {}".format(len(summary["flagged_rows"])))
    return summary


def run_confidence_analysis(raw_rows):
    section("2 -- analyze_point()  CONFIDENCE DISTRIBUTION")
    buckets = defaultdict(int)
    low_confidence = []
    for row_id, name, feature_type, loc_str in raw_rows:
        pt = parse_point_str(loc_str)
        if pt is None:
            continue
        result = analyze_point(pt)
        bucket = "{:03d}-{:03d}%".format(
            int(result.confidence * 10) * 10,
            int(result.confidence * 10) * 10 + 9
        )
        buckets[bucket] += 1
        if result.confidence < 0.5 and result.valid:
            low_confidence.append({
                "id": row_id,
                "name": name,
                "feature_type": feature_type,
                "point": pt,
                "confidence": result.confidence,
                "ambiguous": result.ambiguous,
                "issues": result.issues,
            })
    print("  Confidence distribution:")
    for bucket in sorted(buckets):
        bar = "x" * (buckets[bucket] // 20)
        print("    {}  {:>5}  {}".format(bucket, buckets[bucket], bar))
    if low_confidence:
        print("\n  Low-confidence rows (< 0.50, valid): {}".format(len(low_confidence)))
        for row in low_confidence[:10]:
            print("    id={} | {} | {}".format(row["id"], row["feature_type"], row["name"] or "(unnamed)"))
            print("      point={} | confidence={} | ambiguous={}".format(
                row["point"], row["confidence"], row["ambiguous"]))
    else:
        print("\n  No low-confidence valid points found.")
    return low_confidence


def run_roundtrip_test(raw_rows):
    section("3 -- PointType  BIND + RESULT ROUND-TRIP")
    pt_type = PointType()

    class FakeDialect:
        pass

    bind_fn   = pt_type.bind_processor(FakeDialect())
    result_fn = pt_type.result_processor(FakeDialect(), None)
    passed = 0
    failed = 0
    failures = []
    for row_id, name, feature_type, loc_str in raw_rows:
        pt = parse_point_str(loc_str)
        if pt is None:
            continue
        try:
            bound  = bind_fn(pt)
            result = result_fn(bound)
            assert result is not None
            assert abs(result[0] - pt[0]) < 1e-9, "lng drift: {} vs {}".format(result[0], pt[0])
            assert abs(result[1] - pt[1]) < 1e-9, "lat drift: {} vs {}".format(result[1], pt[1])
            passed += 1
        except Exception as e:
            failed += 1
            failures.append({"id": row_id, "point": pt, "error": str(e)})
    print("  Passed: {}".format(passed))
    print("  Failed: {}".format(failed))
    if failures:
        print("\n  Failures:")
        for f in failures[:10]:
            print("    id={} | {} | {}".format(f["id"], f["point"], f["error"]))
    else:
        print("  All points survive bind + result round-trip with no precision drift.")
    return failures


def run_strict_mode_test(raw_rows):
    section("4 -- PointType(strict=True)  AMBIGUOUS DETECTION")
    strict_type = PointType(strict=True, strict_mode="warn")

    class FakeDialect:
        pass

    bind_fn = strict_type.bind_processor(FakeDialect())
    strict_warnings = []
    for row_id, name, feature_type, loc_str in raw_rows:
        pt = parse_point_str(loc_str)
        if pt is None:
            continue
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            try:
                bind_fn(pt)
            except ValueError:
                pass
            for w in caught:
                if issubclass(w.category, UserWarning):
                    strict_warnings.append({
                        "id": row_id,
                        "name": name,
                        "feature_type": feature_type,
                        "point": pt,
                        "warning": str(w.message),
                    })
    if strict_warnings:
        print("  Strict mode flagged {} ambiguous points:".format(len(strict_warnings)))
        for w in strict_warnings[:10]:
            print("    id={} | {} | {}".format(w["id"], w["feature_type"], w["name"] or "(unnamed)"))
            print("      point={}".format(w["point"]))
    else:
        print("  No ambiguous points detected in strict mode.")
        print("  (Utah longitudes ~-109 to -114 are outside [-90,90] -- unambiguous by definition)")
    return strict_warnings


def run_earth_distance_query(database_url):
    section("5 -- earth_distance()  NEAREST FEATURES TO PARK CITY")
    engine = create_engine(database_url)
    try:
        with Session(engine) as session:
            results = (
                session.query(
                    SkiFeature.id,
                    SkiFeature.name,
                    SkiFeature.feature_type,
                    SkiFeature.location,
                    SkiFeature.location.earth_distance(PARK_CITY).label("dist_miles"),
                )
                .filter(SkiFeature.location.isnot(None))
                .order_by("dist_miles")
                .limit(10)
                .all()
            )
            print("  Origin: Park City base ({}, {})".format(PARK_CITY_LAT, PARK_CITY_LNG))
            print("  10 nearest ski features:\n")
            print("  {:>6}  {:<14}  name".format("mi", "type"))
            print("  {:>6}  {:<14}  {}".format("--", "--------------", "----------------------------"))
            for row in results:
                dist = row.dist_miles or 0
                name = row.name or "(unnamed)"
                print("  {:>6.2f}  {:<14}  {}".format(dist, row.feature_type or "", name))
    except Exception as e:
        err = str(e)
        if "<@>" in err or "operator" in err.lower() or "earth_distance" in err.lower():
            print("  earthdistance extension not enabled on this Neon database.")
            print("  To enable:")
            print("    CREATE EXTENSION IF NOT EXISTS cube;")
            print("    CREATE EXTENSION IF NOT EXISTS earthdistance;")
        else:
            print("  Query failed: {}".format(e))
    finally:
        engine.dispose()


def run_feature_breakdown(raw_rows):
    section("6 -- DATASET BREAKDOWN BY FEATURE TYPE")
    counts = defaultdict(int)
    named  = defaultdict(int)
    for _, name, feature_type, _ in raw_rows:
        ft = feature_type or "unknown"
        counts[ft] += 1
        if name:
            named[ft] += 1
    print("  {:<16}  {:>6}  {:>6}  {:>7}".format("feature_type", "count", "named", "named%"))
    print("  {:<16}  {:>6}  {:>6}  {:>7}".format("------------", "-----", "-----", "------"))
    for ft in sorted(counts, key=lambda x: -counts[x]):
        pct = (named[ft] / counts[ft]) * 100
        print("  {:<16}  {:>6}  {:>6}  {:>6.1f}%".format(ft, counts[ft], named[ft], pct))


def main():
    print("\n" + "*" * 58)
    print("  Utah Ski Features -- Full Library Audit")
    print("  sqlalchemy-postgres-point  |  GPS.ski_features")
    print("*" * 58)
    print("\nFetching rows from Neon...")
    raw_rows = fetch_raw_rows(DATABASE_URL)
    print("  {} rows loaded".format(len(raw_rows)))

    summary      = run_batch_validation(raw_rows)
    low_conf     = run_confidence_analysis(raw_rows)
    rt_failures  = run_roundtrip_test(raw_rows)
    strict_flags = run_strict_mode_test(raw_rows)
    run_earth_distance_query(DATABASE_URL)
    run_feature_breakdown(raw_rows)

    section("AUDIT COMPLETE")
    print("  Rows audited:          {}".format(len(raw_rows)))
    print("  Batch validation:      {:.1f}% clean".format(
        100 - summary["likely_swapped_pct"] - summary["invalid_pct"] - summary["ambiguous_pct"]))
    print("  Round-trip failures:   {}".format(len(rt_failures)))
    print("  Strict mode flags:     {}".format(len(strict_flags)))
    print("  Low-confidence points: {}".format(len(low_conf)))

    report = {
        "batch_summary": summary,
        "roundtrip_failures": rt_failures,
        "strict_warnings": strict_flags,
        "low_confidence": low_conf,
    }
    with open("audit_report.json", "w") as f:
        json.dump(report, f, indent=2, default=str)
    print("\n  Full report saved to audit_report.json")


if __name__ == "__main__":
    main()
