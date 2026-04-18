from sqlalchemy.orm import Session

from chalicelib.new.company.infra.company_repository_sa import CompanyRepositorySA
from chalicelib.new.query.domain.enums.query_state import QueryState
from chalicelib.new.query.domain.enums.technology import SATDownloadTechnology
from chalicelib.new.shared.infra.message.sqs_company import SQSUpdaterQuery
from chalicelib.new.utils.datetime import utc_now
from chalicelib.new.ws_sat.infra.query_updater_ws import QueryUpdaterWS
from chalicelib.schema.models.tenant.sat_query import SATQuery
from tests.fixtures.factories.sat_query import SATQueryFactory


def get_query_from_db(session: Session) -> SATQuery:
    query = (
        session.query(SATQuery)
        .filter(SATQuery.technology == SATDownloadTechnology.WebService)
        .one()
    )
    return query


def get_default_sat_query_factory() -> SATQuery:
    return SATQueryFactory.build(
        name="SAT Query",
        identifier="00000000-0000-0000-0000-000000000001",
        origin_identifier=None,
        updated_at=None,
        packages=(),
        state=QueryState.DRAFT,
        technology=SATDownloadTechnology.WebService,
        request_type="BOTH",
    )


def get_updater_instance(bus, company_session: Session) -> QueryUpdaterWS:
    # Preparamos el updater
    company_repo = CompanyRepositorySA(session=company_session)

    return QueryUpdaterWS(
        bus=bus,
        company_session=company_session,
        company_repo=company_repo,
    )


def test_basic_ws_updater(bus, company, company_session: Session):
    sat_query = get_default_sat_query_factory()

    # Agregamos una query a la base de datos
    company_session.add(sat_query)

    # Creamos el 'request' que sería el message del SQS
    company_identifier = company.identifier

    request = SQSUpdaterQuery(
        query_identifier=sat_query.identifier,
        state=QueryState.SENT,
        request_type="BOTH",
        company_identifier=company_identifier,
        state_update_at=utc_now(),
        name=sat_query.name,
        sent_date=sat_query.sent_date,
    )

    # Obtnemos el updater
    updater = get_updater_instance(bus, company_session)

    result = get_query_from_db(company_session)

    # validamos que la query no tiene updated_at porque así lo definimos en la factory
    assert result.updated_at is None

    # Procesamos la actualización
    updater.process_update(request)

    result = get_query_from_db(company_session)

    # Validamos que la query tiene updated_at resultado de la actualización
    assert result.updated_at
    assert result.updated_at == request.state_update_at
    assert result.state == QueryState.SENT


def test_multiple_updates(bus, company, company_session: Session):
    sat_query = get_default_sat_query_factory()

    # Agregamos una query a la base de datos
    company_session.add(sat_query)

    # Creamos el 'request' que sería el message del SQS
    company_identifier = company.identifier

    request_1 = SQSUpdaterQuery(
        query_identifier=sat_query.identifier,
        state=QueryState.SENT,
        request_type="CFDI",
        company_identifier=company_identifier,
        state_update_at=utc_now(),
        name=sat_query.name,
        sent_date=sat_query.sent_date,
    )

    request_2 = SQSUpdaterQuery(
        query_identifier=sat_query.identifier,
        state=QueryState.TO_DOWNLOAD,
        request_type="CFDI",
        company_identifier=company_identifier,
        state_update_at=utc_now(),
        name=sat_query.name,
        sent_date=sat_query.sent_date,
    )

    request_3 = SQSUpdaterQuery(
        query_identifier=sat_query.identifier,
        state=QueryState.DOWNLOADED,
        request_type="CFDI",
        company_identifier=company_identifier,
        state_update_at=utc_now(),
        name=sat_query.name,
        sent_date=sat_query.sent_date,
    )

    # Obtnemos el updater
    updater = get_updater_instance(bus, company_session)

    # Procesamos la actualización en distinto orden en el que sucedieron
    updater.process_update(request_2)
    # result = get_query_from_db(company_session) # TODO: Remove after approve
    updater.process_update(request_3)
    # result = get_query_from_db(company_session) # TODO: Remove after approve
    updater.process_update(request_1)

    result = get_query_from_db(company_session)

    # Validamos que la query tiene updated_at resultado de la actualización
    assert result.updated_at

    assert request_1.state_update_at < request_2.state_update_at
    assert request_2.state_update_at < request_3.state_update_at

    # Validamos que el estado de la query es el último creado (independientemente del orden en que llegaron los requests)
    assert result.state == QueryState.DOWNLOADED
