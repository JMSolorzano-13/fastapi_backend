from tests.fixtures.factories.sat_query import SATQueryFactory


def test_create_sat_query():
    sat_query = SATQueryFactory.build(name="SAT Query")

    assert sat_query.name == "SAT Query"
