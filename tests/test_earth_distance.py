"""
Comprehensive earth_distance integration tests using real GPS coordinates.

Ground truth distances computed with geopy.distance.great_circle, which uses
the same spherical model as PostgreSQL's earthdistance <@> operator. All
assertions use a 1% tolerance to cover Earth radius constant differences
between implementations.

Requires: cube + earthdistance PostgreSQL extensions.
"""

import pytest
from sqlalchemy import select, text
from sqlalchemy.orm import Session

from tests.models import Place


PLACES = {
    "Park City UT":       (-111.4980,  40.6461),
    "Salt Lake City UT":  (-111.8910,  40.7608),
    "New York City NY":   (-74.0060,   40.7128),
    "Los Angeles CA":     (-118.2437,  34.0522),
    "Chicago IL":         (-87.6298,   41.8781),
    "Miami FL":           (-80.1918,   25.7617),
    "London UK":          (-0.1278,    51.5074),
    "Tokyo JP":           (139.6917,   35.6895),
    "Sydney AU":          (151.2093,  -33.8688),
    "Reykjavik IS":       (-21.9426,   64.1466),
    "Singapore SG":       (103.8198,    1.3521),
    "Cape Town ZA":       (18.4241,   -33.9249),
    "Buenos Aires AR":    (-58.3816,  -34.6037),
    "Oslo NO":            (10.7522,    59.9139),
    "Null Island":        (0.0,         0.0),
}

EXPECTED_MILES = {
    ("Park City UT",     "Salt Lake City UT"): 22.06,
    ("New York City NY", "Los Angeles CA"):    2445.56,
    ("New York City NY", "London UK"):         3461.18,
    ("Los Angeles CA",   "Tokyo JP"):          5477.69,
    ("Miami FL",         "Reykjavik IS"):      3681.85,
    ("Singapore SG",     "Sydney AU"):         3918.53,
    ("Cape Town ZA",     "Reykjavik IS"):      7123.15,
    ("New York City NY", "Sydney AU"):         9934.97,
    ("Buenos Aires AR",  "Oslo NO"):           7610.37,
    ("Null Island",      "Null Island"):          0.0,
}

TOLERANCE_PCT = 0.01


@pytest.fixture
def earth_distance_engine(engine):
    with engine.connect() as conn:
        try:
            conn.execute(text("CREATE EXTENSION IF NOT EXISTS cube"))
            conn.execute(text("CREATE EXTENSION IF NOT EXISTS earthdistance"))
            conn.commit()
        except Exception as e:
            pytest.skip(f"cube/earthdistance extensions not available: {e}")
    return engine


@pytest.fixture
def populated_places(earth_distance_engine, create_and_wipe_database):
    with Session(earth_distance_engine) as session:
        for name, (lng, lat) in PLACES.items():
            session.add(Place(name=name, location=(lng, lat)))
        session.commit()
    return earth_distance_engine


def get_distance(engine, from_name, to_name):
    to_lng, to_lat = PLACES[to_name]
    with Session(engine) as session:
        stmt = (
            select(
                Place.__table__.c.location.earth_distance(  # type: ignore[attr-defined]
                    (to_lng, to_lat)
                ).label("dist_miles")
            )
            .where(Place.name == from_name)
        )
        return session.execute(stmt).scalar_one()


@pytest.mark.parametrize("from_place,to_place,expected_miles", [
    (a, b, d) for (a, b), d in EXPECTED_MILES.items()
])
def test_earth_distance_routes(populated_places, from_place, to_place, expected_miles):
    """Each route must be within 1% of geopy great_circle ground truth."""
    dist = get_distance(populated_places, from_place, to_place)
    tolerance = expected_miles * TOLERANCE_PCT if expected_miles > 0 else 0.1
    assert abs(dist - expected_miles) <= tolerance, (
        f"{from_place} -> {to_place}: "
        f"expected {expected_miles:.2f} mi ±{tolerance:.2f}, "
        f"got {dist:.4f} mi"
    )
