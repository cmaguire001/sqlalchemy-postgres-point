from sqlalchemy import select

from tests.models import Place


def test_insert_and_select_point(engine, create_and_wipe_database):
    from sqlalchemy.orm import Session

    with Session(engine) as session:
        p = Place(name="Test", location=(1.0, 2.0))
        session.add(p)
        session.commit()
        session.refresh(p)
        assert p.id is not None

    with Session(engine) as session:
        row = session.get(Place, p.id)
        assert row is not None
        assert row.location == (1.0, 2.0)


def test_distance_operator_compiles(engine, create_and_wipe_database):
    # Only check SQL compilation because the <@> operator may require extensions
    # Access the column attribute from the SQLModel table to get the comparator
    stmt = select(Place.__table__.c.location.earth_distance((3.0, 4.0)))  # type: ignore[attr-defined]
    compiled = stmt.compile(engine)
    sql_text = str(compiled)
    assert "<@>" in sql_text
    assert "places" in sql_text


def test_earth_distance_real_coordinates(engine, create_and_wipe_database):
    """
    Verify earth_distance returns a physically meaningful result using real
    coordinates. Park City, UT to Salt Lake City, UT is ~22 miles via the
    spherical great-circle model that Postgres earthdistance uses.

    Ground truth computed with geopy.distance.great_circle:
        great_circle((40.6461, -111.4980), (40.7608, -111.8910)).miles == 22.06

    The <@> operator requires the cube + earthdistance Postgres extensions.
    This test is skipped if those extensions are not available.
    """
    import pytest
    from sqlalchemy import text
    from sqlalchemy.orm import Session

    # Check extensions are available — skip gracefully if not
    with engine.connect() as conn:
        try:
            conn.execute(text("CREATE EXTENSION IF NOT EXISTS cube"))
            conn.execute(text("CREATE EXTENSION IF NOT EXISTS earthdistance"))
            conn.commit()
        except Exception:
            pytest.skip("cube/earthdistance extensions not available in this DB")

    # Real coordinates — stored as (lng, lat) per library convention
    park_city = (-111.4980, 40.6461)   # Park City, UT
    salt_lake = (-111.8910, 40.7608)   # Salt Lake City, UT

    # geopy great_circle ground truth — same spherical model as Postgres <@>
    # great_circle((40.6461, -111.4980), (40.7608, -111.8910)).miles == 22.06
    expected_miles = 22.06
    tolerance = expected_miles * 0.01  # 1% — covers sphere vs R constant differences

    with Session(engine) as session:
        session.add(Place(name="Park City", location=park_city))
        session.add(Place(name="Salt Lake City", location=salt_lake))
        session.commit()

    with Session(engine) as session:
        stmt = (
            select(
                Place.name,
                Place.__table__.c.location.earth_distance(  # type: ignore[attr-defined]
                    salt_lake
                ).label("dist_miles"),
            )
            .where(Place.name == "Park City")
        )
        result = session.execute(stmt).one()

    assert result.name == "Park City"
    assert abs(result.dist_miles - expected_miles) <= tolerance, (
        f"Expected ~{expected_miles} miles ±{tolerance:.2f}, got {result.dist_miles:.4f}"
    )
