from dataclasses import dataclass

from sqlalchemy.orm import Session

from chalicelib.logger import INFO, log
from chalicelib.modules import Modules
from chalicelib.new.shared.domain.primitives import Identifier
from chalicelib.new.stripe.infra import StripeCouponSetter
from chalicelib.schema.models import User as UserORM


@dataclass
class SourceSetter:
    session: Session
    setter: StripeCouponSetter = None

    def set(self, user_id: Identifier, source: str) -> None:
        self.setter = self.setter or StripeCouponSetter()
        user: UserORM = self.session.query(UserORM).filter(UserORM.id == user_id).first()
        if not user:
            raise ValueError(f"User {user_id} does not exist")
        if source:
            if source == user.source_name:
                log(
                    Modules.ACCOUNT,
                    INFO,
                    "SOURCE_ALREADY_SET",
                    {
                        "user_id": user_id,
                        "source": source,
                    },
                )
                return
            self.setter.set_coupon(user.stripe_identifier)
        else:
            self.setter.remove_coupon(user.stripe_identifier)
        user.source_name = source
