"""
ingest_utah_ski.py
Ingests Utahski.geojson into Neon PostgreSQL (GPS database)
- Creates ski_features table with a native POINT column
- Converts all geometry types to a single representative point
- All config via .env (never hardcode credentials)
"""

import json
import os
import sys

import psycopg2
from dotenv import load_dotenv
from shapely.geometry import shape

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    print("ERROR: DATABASE_URL not set in .env")
    sys.exit(1)

GEOJSON_FILE = "Utahski.geojson"

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS ski_features (
    id          SERIAL PRIMARY KEY,
    osm_id      BIGINT,
    osm_type    TEXT,
    feature_type TEXT,
    name        TEXT,
    tags        JSONB,
    location    POINT,
    created_at  TIMESTAMPTZ DEFAULT NOW()
);
"""

INSERT_SQL = """
INSERT INTO ski_features (osm_id, osm_type, feature_type, name, tags, location)
VALUES (%s, %s, %s, %s, %s, POINT(%s, %s));
"""


def classify_feature(props: dict) -> str:
    """Classify an OSM feature into a simple feature_type label."""
    tags = props.get("tags", props)  # support both nested and flat tag layouts
    if tags.get("aerialway"):
        return "lift"
    if tags.get("piste:type"):
        return "piste"
    if tags.get("landuse") == "winter_sports":
        return "resort_area"
    if tags.get("tourism") == "ski_rental":
        return "rental"
    if tags.get("amenity") == "ski_school":
        return "ski_school"
    return "other"


def geometry_to_point(geom_dict: dict):
    """
    Return (lon, lat) for any geometry type.
    - Point      → use directly
    - LineString → midpoint (centroid)
    - Polygon    → centroid
    - Multi*     → centroid of full geometry
    Returns None if geometry is missing or invalid.
    """
    if not geom_dict:
        return None
    try:
        geom = shape(geom_dict)
        centroid = geom.centroid
        return (centroid.x, centroid.y)
    except Exception as e:
        print(f"  [WARN] Could not parse geometry: {e}")
        return None


def extract_tags(props: dict) -> dict:
    """Pull all OSM tags into a flat dict for jsonb storage."""
    # Overpass GeoJSON nests tags under a 'tags' key
    return props.get("tags", {}) or {}


def main():
    print(f"Loading {GEOJSON_FILE}...")
    with open(GEOJSON_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)

    features = data.get("features", [])
    print(f"  {len(features)} features found")

    print("Connecting to Neon (GPS)...")
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()

    print("Creating ski_features table if not exists...")
    cur.execute(CREATE_TABLE_SQL)
    conn.commit()

    inserted = 0
    skipped = 0

    print("Ingesting features...")
    for feat in features:
        props = feat.get("properties") or {}
        geom = feat.get("geometry")

        point = geometry_to_point(geom)
        if point is None:
            skipped += 1
            continue

        lon, lat = point

        osm_id = props.get("@id") or props.get("id")
        # Strip type prefix from OSM id strings like "way/123456"
        osm_type = None
        if osm_id and isinstance(osm_id, str) and "/" in osm_id:
            osm_type, osm_id = osm_id.split("/", 1)
        try:
            osm_id = int(osm_id) if osm_id else None
        except (ValueError, TypeError):
            osm_id = None

        tags = extract_tags(props)
        name = props.get("name") or tags.get("name")
        feature_type = classify_feature(props)

        cur.execute(INSERT_SQL, (
            osm_id,
            osm_type,
            feature_type,
            name,
            json.dumps(tags),
            lon,
            lat,
        ))
        inserted += 1

        if inserted % 500 == 0:
            print(f"  ...{inserted} rows inserted")

    conn.commit()
    cur.close()
    conn.close()

    print(f"\nDone. {inserted} rows inserted, {skipped} skipped (no geometry).")
    print("Table: ski_features | Column: location (POINT)")


if __name__ == "__main__":
    main()
