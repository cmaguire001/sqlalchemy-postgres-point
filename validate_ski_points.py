"""
validate_ski_points.py
Pulls location column from ski_features and runs it through
the sqlalchemy-postgres-point validate_points() batch validator.
"""

import os
import sys
import json
import psycopg2
from dotenv import load_dotenv
from sqlalchemy_postgres_point import validate_points

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    print("ERROR: DATABASE_URL not set in .env")
    sys.exit(1)


def parse_point(pg_point_str):
    """
    Parse PostgreSQL POINT string '(lon,lat)' into a (float, float) tuple.
    Handles standard and scientific notation formats.
    """
    if pg_point_str is None:
        return None
    s = str(pg_point_str).strip().strip("()")
    parts = s.split(",")
    if len(parts) != 2:
        return None
    try:
        return (float(parts[0]), float(parts[1]))
    except ValueError:
        return None


def main():
    print("Connecting to Neon (GPS)...")
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()

    print("Fetching ski_features location column...")
    cur.execute("""
        SELECT id, name, feature_type, location::text
        FROM ski_features
        ORDER BY id;
    """)
    rows = cur.fetchall()
    cur.close()
    conn.close()

    print(f"  {len(rows)} rows fetched\n")

    # Build list of (lon, lat) tuples for validate_points()
    points = []
    meta = []  # keep row metadata aligned with points list
    parse_failures = 0

    for row_id, name, feature_type, location_str in rows:
        pt = parse_point(location_str)
        if pt is None:
            parse_failures += 1
            continue
        points.append(pt)
        meta.append({
            "db_id": row_id,
            "name": name,
            "feature_type": feature_type,
            "raw": location_str,
        })

    if parse_failures:
        print(f"[WARN] {parse_failures} rows could not be parsed and were skipped\n")

    print(f"Running validate_points() on {len(points)} points...\n")
    summary = validate_points(points)

    # Print summary
    print("=" * 55)
    print("  GPS VALIDATION REPORT — Utah Ski Features")
    print("=" * 55)
    print(f"  Total rows:       {summary['total_rows']}")
    print(f"  Valid:            {100 - summary['likely_swapped_pct'] - summary['invalid_pct'] - summary['ambiguous_pct']:.1f}%")
    print(f"  Swapped:          {summary['likely_swapped_pct']:.1f}%")
    print(f"  Invalid:          {summary['invalid_pct']:.1f}%")
    print(f"  Ambiguous:        {summary['ambiguous_pct']:.1f}%")
    print("=" * 55)

    # Print flagged rows with metadata
    flagged = summary.get("flagged_rows", [])
    if flagged:
        print(f"\nFlagged rows ({len(flagged)}):\n")
        for flag in flagged:
            idx = flag.get("index")
            if idx is not None and idx < len(meta):
                m = meta[idx]
                print(f"  [{flag.get('status')}] id={m['db_id']} | {m['feature_type']} | {m['name'] or '(unnamed)'}")
                print(f"         value={flag.get('value')} | {flag.get('issue', '')}")
                if flag.get("recommendation"):
                    print(f"         → {flag['recommendation']}")
                print()
    else:
        print("\n✓ No flagged rows — all points passed validation.")

    # Save full summary to JSON for reference
    output_file = "validation_report.json"
    with open(output_file, "w") as f:
        json.dump(summary, f, indent=2, default=str)
    print(f"\nFull report saved to {output_file}")


if __name__ == "__main__":
    main()
