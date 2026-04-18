from sqlalchemy.orm import Session

from chalicelib.controllers.common import CommonController
from chalicelib.new.shared.domain.primitives import Identifier
from chalicelib.schema.models import UserConfig


class UserConfigController(CommonController):
    model = UserConfig

    @classmethod
    def get_config(cls, user_identifier: Identifier, *, company_session: Session) -> UserConfig:
        """Get user config for a company."""
        return company_session.query(UserConfig).get(user_identifier)

    @classmethod
    def _create_config(
        cls,
        user_identifier: Identifier,
        *,
        company_session: Session,
    ) -> UserConfig:
        """Create a new user config."""
        config = UserConfig(user_identifier=user_identifier)
        company_session.add(config)
        return config

    @classmethod
    def set_config(
        cls,
        user_identifier: Identifier,
        config: str,
        *,
        company_session: Session,
    ) -> UserConfig:
        """Set a user config."""
        user_config = cls.get_config(
            user_identifier, company_session=company_session
        ) or cls._create_config(user_identifier, company_session=company_session)
        user_config.data = config
        return user_config
