import datetime
from dataclasses import dataclass
from datetime import date

from sqlalchemy.orm import Session

from chalicelib.new.pasto.event import ADDDataSync
from chalicelib.new.shared.domain.event.event_bus import EventBus
from chalicelib.new.shared.domain.event.event_type import EventType
from chalicelib.new.shared.domain.primitives import Identifier
from chalicelib.new.utils.datetime import mx_now
from chalicelib.schema.models import ADDSyncRequest


@dataclass
class ADDSyncRequester:
    company_session: Session
    bus: EventBus

    def request(
        self,
        company_identifier: Identifier,
        pasto_company_identifier: Identifier,
        pasto_token: str,
        start: date | None = None,
        end: date | None = None,
        manually_triggered=False,
    ) -> ADDSyncRequest:
        if not start or not end:
            start, end = self.add_time_window_to_sync()

        request = ADDSyncRequest(
            start=start,
            end=end,
            manually_triggered=manually_triggered,
        )
        self.company_session.add(request)
        self.company_session.commit()

        self.bus.publish(
            EventType.ADD_SYNC_REQUEST_CREATED,
            ADDDataSync(
                request_identifier=request.identifier,
                company_identifier=company_identifier,
                pasto_company_identifier=pasto_company_identifier,
                start=start,
                end=end,
                pasto_worker_token=pasto_token,
            ),
        )

        return request

    def add_time_window_to_sync(self):
        """Return the start and end dates for the ADD sync request
        end = Mexican current date
        start = Mexican current start of month, or previous month if today is the 1st, 2nd or 3rd
        """
        today = mx_now().date()

        start = datetime.date(today.year, today.month, 1)

        if today.day <= 3:
            start = start.replace(month=start.month - 1)

        return start, today
