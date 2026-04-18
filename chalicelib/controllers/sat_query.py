from datetime import datetime

from sqlalchemy import func
from sqlalchemy.orm import Session

from chalicelib.controllers.common import CommonController
from chalicelib.schema.models.tenant import SATQuery


class SATQueryController(CommonController):
    model = SATQuery

    @classmethod
    def get_fist_and_last_query(cls, company_identifier, session: Session):
        first, last = cls._get_first_and_last_date(company_identifier, session=session)
        return first, last

    @staticmethod
    def _get_first_and_last_date(
        company_identifier, session: Session
    ):  # TODO: Put start and end about first query when be metadata
        # first query
        query = (
            session.query(
                func.min(SATQuery.start).label("first"),
                func.max(SATQuery.end).label("last"),
            )
            .filter(
                SATQuery.request_type == "METADATA",
            )
            .group_by(SATQuery.created_at)
            .order_by(SATQuery.created_at.asc())
            .limit(1)
            .first()
        )
        if not query:
            return None, None
        first = (
            datetime.combine(query.first, datetime.min.time()).isoformat() if query.first else None
        )
        last = datetime.combine(query.last, datetime.min.time()).isoformat() if query.last else None

        return first, last
