import uuid

from tests.fixtures.factories.pasto import PastoCompanyFactory


def test_create_pasto_company():
    pasto = PastoCompanyFactory.build(
        name="Test Pasto Company",
        workspace_identifier=str(uuid.uuid4()),
        pasto_company_id=1,
    )

    assert pasto.name == "Test Pasto Company"
