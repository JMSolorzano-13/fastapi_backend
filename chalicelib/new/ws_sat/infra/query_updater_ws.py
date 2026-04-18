from dataclasses import dataclass
from logging import WARNING

from sqlalchemy import update
from sqlalchemy.orm import Session

from chalicelib.logger import log
from chalicelib.modules import Modules
from chalicelib.new.company.infra.company_repository_sa import CompanyRepositorySA
from chalicelib.new.config.infra import envars
from chalicelib.new.query.domain.enums.query_state import QueryState
from chalicelib.new.query.domain.enums.request_type import RequestType
from chalicelib.new.shared.domain.event.event_bus import EventBus
from chalicelib.new.shared.infra.message.sqs_company import SQSUpdaterQuery
from chalicelib.schema.models.tenant import SATQuery as SATQueryORM


@dataclass
class QueryUpdaterWS:
    """Centralizador de todas las actualizaciones de base de datos para SAT WebService"""

    bus: EventBus
    company_session: Session
    company_repo: CompanyRepositorySA

    def process_update(self, request: SQSUpdaterQuery) -> None:
        """Procesa una solicitud de actualización"""
        self._update(request)

    def _update(self, request: SQSUpdaterQuery):
        data = {"state": request.state, "updated_at": request.state_update_at}

        if request.state == QueryState.SENT:
            data |= {
                "name": request.name,
                "sent_date": request.sent_date,
            }

        elif request.state in (QueryState.TO_DOWNLOAD, QueryState.DOWNLOADED):
            data |= {
                "cfdis_qty": request.cfdis_qty,
                "packages": request.packages,
            }

        self.update_query_state(data, request)
        self.mark_too_big_if_needed(request)

    def update_query_state(self, data, request):
        """Realiza UPSERT usando state_update_at para evitar condiciones de carrera"""
        self.company_session.execute(
            update(SATQueryORM)
            .where(
                SATQueryORM.identifier == request.query_identifier,
                (SATQueryORM.updated_at < request.state_update_at)
                | (SATQueryORM.updated_at.is_(None)),
            )
            .values(**data)
        )

    def mark_too_big_if_needed(self, request: SQSUpdaterQuery):
        """Marca empresa con límite excedido si es query de metadata demasiado grande"""
        if request.state != QueryState.ERROR_TOO_BIG:
            return

        # Solo para queries de CFDI, registrar warning y salir
        if request.request_type == RequestType.CFDI:
            log(
                Modules.SAT_WS_VERIFY,
                WARNING,
                "TOO_MANY_CFDI_IN_QUERY",
                {
                    "max-size": envars.control.MAX_CFDI_QTY_IN_QUERY,
                    "body": request,
                },
            )
            return

        company = self.company_repo.get_by_identifier(request.company_identifier)
        self.company_repo.update(
            company,
            {"exceed_metadata_limit": True},
        )
