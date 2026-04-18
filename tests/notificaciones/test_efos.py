from datetime import datetime, timedelta

from sqlalchemy.orm import Session

from chalicelib.controllers.notification import NotificationController, NotificationSectionType
from chalicelib.schema.models.company import Company
from chalicelib.schema.models.efos import EFOS, EFOS_DATE_FORMAT_PYTHON
from chalicelib.schema.models.tenant.cfdi import CFDI


def _seed_efos_and_cfdis_for_notifications(
    company_identifier: str, company_session: Session, base_date: datetime
) -> dict[str, list[str]]:
    """Seed EFOS records and CFDIs for testing EFOS notification filtering"""
    now = base_date
    yesterday = now - timedelta(days=1)
    two_weeks_ago = now - timedelta(days=14)

    # EFOS 1: ALLEGED state with recent sat_publish_alleged_date (should match)
    efos_alleged_recent = EFOS(
        no=1,
        name="efos_alleged_recent",
        rfc="RFC001",
        state=EFOS.StateEnum.ALLEGED,
        sat_publish_alleged_date=yesterday.strftime(EFOS_DATE_FORMAT_PYTHON),
    )

    # EFOS 2: ALLEGED state with old sat_publish_alleged_date (should NOT match if CFDI is also old)
    efos_alleged_old = EFOS(
        no=2,
        name="efos_alleged_old",
        rfc="RFC002",
        state=EFOS.StateEnum.ALLEGED,
        sat_publish_alleged_date=two_weeks_ago.strftime(EFOS_DATE_FORMAT_PYTHON),
    )

    # EFOS 3: DEFINITIVE state (should NOT match regardless of date)
    efos_definitive = EFOS(
        no=3,
        name="efos_definitive",
        rfc="RFC003",
        state=EFOS.StateEnum.DEFINITIVE,
        sat_publish_alleged_date=yesterday.strftime(EFOS_DATE_FORMAT_PYTHON),
    )

    company_session.add_all([efos_alleged_recent, efos_alleged_old, efos_definitive])
    company_session.flush()

    # CFDI 1: Received (not issued), with EFOS in ALLEGED state and recent EFOS date (should appear)
    cfdi_with_efos_recent = CFDI.demo(
        is_issued=False,
        RfcEmisor=efos_alleged_recent.rfc,
    )

    # CFDI 2: Received, with EFOS in ALLEGED state but both CFDI and EFOS dates are old (should NOT appear)
    cfdi_with_efos_old = CFDI.demo(
        created_at=two_weeks_ago,  # Old CFDI date
        is_issued=False,
        RfcEmisor=efos_alleged_old.rfc,
    )

    # CFDI 3: Received, with EFOS in DEFINITIVE state (should NOT appear)
    cfdi_with_efos_definitive = CFDI.demo(
        is_issued=False,
        RfcEmisor=efos_definitive.rfc,
    )

    # CFDI 4: Received, with EFOS in ALLEGED state, recent CFDI date (should appear)
    cfdi_with_efos_recent_cfdi = CFDI.demo(
        is_issued=False,
        created_at=yesterday,  # Recent CFDI date
        RfcEmisor=efos_alleged_old.rfc,
    )

    # CFDI 5: Issued (not received), with EFOS in ALLEGED state (should NOT appear - filter is ~CFDI.is_issued)
    cfdi_issued_with_efos = CFDI.demo(
        is_issued=True,
        created_at=yesterday,
        RfcEmisor=efos_alleged_recent.rfc,
    )

    # CFDI 6: Received, with EFOS in ALLEGED state but Estatus=False (should NOT appear)
    cfdi_with_efos_inactive = CFDI.demo(
        is_issued=False,
        Estatus=False,  # Inactive
        created_at=yesterday,
        RfcEmisor=efos_alleged_recent.rfc,
    )

    company_session.add_all(
        [
            cfdi_with_efos_recent,
            cfdi_with_efos_old,
            cfdi_with_efos_definitive,
            cfdi_with_efos_recent_cfdi,
            cfdi_issued_with_efos,
            cfdi_with_efos_inactive,
        ]
    )
    company_session.flush()

    # Return UUIDs for test assertions
    return {
        "should_appear": [
            cfdi_with_efos_recent.UUID,
            cfdi_with_efos_recent_cfdi.UUID,
        ],
        "should_not_appear": [
            cfdi_with_efos_old.UUID,
            cfdi_with_efos_definitive.UUID,
            cfdi_issued_with_efos.UUID,
            cfdi_with_efos_inactive.UUID,
        ],
    }


def test_efos_notification_filtering_logic(
    session: Session, company: Company, company_session: Session
):
    """Test that EFOS notification filtering correctly identifies CFDIs based on state and dates"""
    company_obj = company

    # Use a fixed base date for deterministic testing
    base_date = datetime.fromisoformat("2024-01-10")

    # Seed EFOS and CFDI data with the base date
    test_uuids = _seed_efos_and_cfdis_for_notifications(
        company_obj.identifier, company_session, base_date
    )

    controller = NotificationController(session=session)

    # Filter date is 3 days before base date
    # With this filter:
    # - base_date (now) is within filter range
    # - yesterday (base_date - 1 day) is within filter range (recent)
    # - last_week (base_date - 7 days) is outside filter range (old)
    # - two_weeks_ago (base_date - 14 days) is outside filter range (old)
    filter_date = base_date - timedelta(days=3)

    # Get the EFOS notification section
    efos_section = controller.new_cfdis_with_efos(company_session, filter_date)

    # Verify section properties
    assert efos_section.name == "cfdis_with_efos"
    assert efos_section.type == NotificationSectionType.EFOS

    # Get UUIDs of CFDIs that appeared in the section
    cfdi_uuids = {cfdi.UUID for cfdi in efos_section.cfdis}

    # Should include: CFDIs with ALLEGED state and (recent CFDI date OR recent EFOS date)
    for uuid in test_uuids["should_appear"]:
        assert uuid in cfdi_uuids, f"Expected UUID {uuid} to appear in EFOS notifications"

    # Should NOT include:
    for uuid in test_uuids["should_not_appear"]:
        assert uuid not in cfdi_uuids, f"UUID {uuid} should not appear in EFOS notifications"

    # Verify the count matches the expected number of CFDIs that should appear
    assert len(efos_section.cfdis) == len(test_uuids["should_appear"])
