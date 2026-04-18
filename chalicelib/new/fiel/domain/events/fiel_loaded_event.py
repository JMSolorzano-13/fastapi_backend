from pydantic import ConfigDict

from chalicelib.new.shared.domain.event.event import CompanyEvent
from chalicelib.schema.models import User


class FIELLoadedEvent(CompanyEvent):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    user: User
    wid: int
    cid: int
    rfc: str
