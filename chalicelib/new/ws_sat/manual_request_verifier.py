from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Literal

from sqlalchemy.orm import Session

from chalicelib.controllers.permission import Ability, PermissionController
from chalicelib.new.config.infra import envars
from chalicelib.new.query.domain.enums import FinalOkStates, FinalStates, RequestType
from chalicelib.new.utils.datetime import today_mx_in_utc, utc_now, utc_to_mx
from chalicelib.schema.models import (
    User as UserORM,
)
from chalicelib.schema.models.company import Company as CompanyORM
from chalicelib.schema.models.tenant import (
    CFDI as CFDIORM,
)
from chalicelib.schema.models.tenant import (
    SATQuery as SATQueryORM,
)

QUERIES_PER_REQUEST = 1  # TODO move accord to app.py


@dataclass
class CanRequestManualResponse:
    status: Literal["ok", "error"]
    reason: str = ""
    can_request: bool = False
    last_manual_sync_requested: datetime | None = None
    last_sync_processed: datetime | None = None
    all_cfdis_processed: bool = True


@dataclass
class ManualRequestVerifier:
    company_session: Session

    def _get_last_manual_sync_requested(self) -> datetime | None:
        if last_manual_sync := (
            self.company_session.query(SATQueryORM)
            .filter(
                SATQueryORM.is_manual,
                SATQueryORM.request_type == RequestType.METADATA,
            )
            .order_by(SATQueryORM.created_at.desc())
            .first()
        ):
            return utc_to_mx(last_manual_sync.created_at)
        return None

    def _get_all_cfdis_processed(self) -> bool:
        if self.company_session.query(CFDIORM.UUID).first() is None:
            return False

        have_not_xml = (
            self.company_session.query(CFDIORM.UUID)
            .filter(
                CFDIORM.Estatus,
                ~CFDIORM.from_xml,
                ~CFDIORM.is_too_big,
            )
            .first()
        )
        return not bool(have_not_xml)

    def _get_last_sync_processed(self) -> datetime | None:
        if last_sync := (
            self.company_session.query(SATQueryORM)
            .filter(
                SATQueryORM.state.in_(FinalOkStates),
            )
            .order_by(SATQueryORM.created_at.desc())
            .first()
        ):
            return last_sync.created_at
        return None

    def can_request_manual_sync(
        self,
        user: UserORM,
        company: CompanyORM,
        session: Session,
        limit_request_types: Iterable[RequestType] | None = None,
        *,
        is_owner: bool = False,
    ) -> CanRequestManualResponse:
        last_manual_sync_requested = self._get_last_manual_sync_requested()
        last_sync_processed = self._get_last_sync_processed()
        all_cfdis_processed = self._get_all_cfdis_processed()

        if company.is_especial():
            return CanRequestManualResponse(
                status="ok",
                reason="",
                can_request=True,
                last_manual_sync_requested=last_manual_sync_requested,
                last_sync_processed=last_sync_processed,
                all_cfdis_processed=all_cfdis_processed,
            )

        can_request, reason = self._get_can_request_and_reason(
            user,
            company,
            session,
            limit_request_types=limit_request_types,
            is_owner=is_owner,
        )
        status = "ok" if can_request else "error"
        return CanRequestManualResponse(
            status=status,
            reason=reason,
            can_request=can_request,
            last_manual_sync_requested=last_manual_sync_requested,
            last_sync_processed=last_sync_processed,
            all_cfdis_processed=all_cfdis_processed,
        )

    def _get_can_request_and_reason(
        self,
        user: UserORM,
        company: CompanyORM,
        session: Session,
        *,
        limit_request_types: Iterable[RequestType] | None = None,
        is_owner: bool = False,
    ) -> tuple[bool, str]:
        abilities = PermissionController.get_abilities(user, company, session=session)
        # Allow owner if explicitly provided by the caller context
        if Ability.SATSync not in abilities and not is_owner:
            return False, (
                f"User {user.identifier} does not have permission to request "
                f"manual sync for company {company.identifier}"
            )

        if not (company.active and company.have_certificates and company.workspace.is_active):
            return False, (
                f"Company {company.identifier} is not active, does not have "
                f"certificates or its workspace is not active"
            )

        # Daily limit check
        request_types = (
            tuple(limit_request_types)
            if limit_request_types is not None
            else (RequestType.METADATA,)
        )
        today = today_mx_in_utc()
        manual_syncs = self.company_session.query(SATQueryORM.identifier).filter(
            SATQueryORM.is_manual,
        )
        manual_syncs_used_today = manual_syncs.filter(
            SATQueryORM.created_at >= today,
            SATQueryORM.request_type.in_(request_types),
        ).count()
        if manual_syncs_used_today >= envars.MAX_MANUAL_SYNC_PER_DAY * QUERIES_PER_REQUEST:
            return False, (
                f"Company {company.identifier} has reached the maximum number of "
                f"manual syncs per day ({envars.MAX_MANUAL_SYNC_PER_DAY})"
            )

        # In-progress guard (same request type set)
        delta_max = timedelta(hours=2)
        if manual_syncs.filter(
            SATQueryORM.created_at >= utc_now() - delta_max,
            ~SATQueryORM.state.in_(FinalStates),
            SATQueryORM.request_type.in_(request_types),
        ).first():
            return False, (
                f"Company {company.identifier} have a manual sync in progress "
                f"created in the last {delta_max}"
            )
        return True, ""
