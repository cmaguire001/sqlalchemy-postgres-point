"""
ingest_alps.py
Ingests Alps.geojson into Neon PostgreSQL (GPS database)
Table: alps_features
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

GEOJSON_FILE = "Alps.geojson"

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS alps_features (
    id           SERIAL PRIMARY KEY,
    osm_id       BIGINT,
    osm_type     TEXT,
    feature_type TEXT,
    name         TEXT,
    tags         JSONB,
    location     POINT,
    created_at   TIMESTAMPTZ DEFAULT NOW()
);
"""

INSERT_SQL = """
INSERT INTO alps_features (osm_id, osm_type, feature_type, name, tags, location)
VALUES (%s, %s, %s, %s, %s, POINT(%s, %s));
"""


def classify_feature(props):
    tags = props.get("tags", props)
    if tags.get("aerialway"):
        return "lift"
    if tags.get("piste:type"):
        return "piste"
    if tags.get("landuse") == "winter_sports":
        return "resort_area"
    return "other"


def geometry_to_point(geom_dict):
    if not geom_dict:
        return None
    try:
        geom = shape(geom_dict)
        centroid = geom.centroid
        return (centroid.x, centroid.y)
    except Exception as e:
        print("  [WARN] Could not parse geometry: {}".format(e))
        return None


def extract_tags(props):
    return props.get("tags", {}) or {}


def main():
    print("Loading {}...".format(GEOJSON_FILE))
    with open(GEOJSON_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)

    features = data.get("features", [])
    print("  {} features found".format(len(features)))

    print("Connecting to Neon (GPS)...")
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()

    print("Creating alps_features table if not exists...")
    cur.execute(CREATE_TABLE_SQL)
    conn.commit()

    inserted = 0
    skipped = 0

    print("Ingesting features...")
    for feat in features:
        props = feat.get("properties") or {}
        geom = feat.get("geometry")

        # out center returns a Point geometry directly
        point = geometry_to_point(geom)
        if point is None:
            skipped += 1
            continue

        lon, lat = point

        osm_id = props.get("@id") or props.get("id")
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

        if inserted % 100 == 0:
            print("  ...{} rows inserted".format(inserted))

    conn.commit()
    cur.close()
    conn.close()

    print("\nDone. {} rows inserted, {} skipped (no geometry).".format(inserted, skipped))
    print("Table: alps_features | Column: location (POINT)")


if __name__ == "__main__":
    main()
