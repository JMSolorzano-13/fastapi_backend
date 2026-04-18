from sqlalchemy.orm import Session

from chalicelib.controllers import ensure_dict_by_ids, scale_to_super_user
from chalicelib.controllers.common import CommonController
from chalicelib.controllers.user import UserController
from chalicelib.logger import log_in
from chalicelib.schema.models import Company, NotificationConfig, User, Workspace

NotificationTypeEnum = NotificationConfig.NotificationTypeEnum


class NotificationConfigController(CommonController):
    model = NotificationConfig

    @staticmethod
    def get_configs(
        user: User, workspace: Workspace, *, session: Session
    ) -> set[NotificationTypeEnum]:
        session.add(user)
        session.add(workspace)
        records = (
            session.query(NotificationConfig.notification_type)
            .filter(
                NotificationConfig.user_id == user.id,
                NotificationConfig.workspace_id == workspace.id,
            )
            .all()
        )
        return {record[0] for record in records}

    @staticmethod
    def add(
        notification_types: set[NotificationTypeEnum],
        user: User,
        workspace: Workspace,
        *,
        session,
        context,
    ) -> None:
        if not notification_types:
            return
        for notification_type in notification_types:
            NotificationConfigController.create(
                {
                    "user_id": user.id,
                    "workspace_id": workspace.id,
                    "notification_type": notification_type,
                },
                session=session,
                context=context,
            )

    @staticmethod
    def remove(
        notification_types: set[NotificationTypeEnum], user: User, workspace: Workspace, *, session
    ) -> None:
        if not notification_types:
            return
        log_in(notification_types)
        session.query(NotificationConfig).filter(
            NotificationConfig.user_id == user.id,
            NotificationConfig.workspace_id == workspace.id,
            NotificationConfig.notification_type.in_(notification_types),
        ).delete()

    @classmethod
    @ensure_dict_by_ids
    def set_notification_types(
        cls,
        notifications_by_user: dict[int, list[str]],
        workspace: Workspace,
        *,
        session: Session,
        context=None,
    ) -> list[NotificationConfig]:
        enroller = context["user"]
        session.add(enroller)
        session.add(workspace)
        NotificationConfigController.ensure_can_set_notification_config(
            enroller, workspace, session=session
        )
        for user_id, type_strings in notifications_by_user.items():
            user_id = int(user_id)
            scale_to_super_user(context)
            user = UserController.get(user_id, session=session, context=context)
            current_configs = NotificationConfigController.get_configs(
                user, workspace, session=session
            )
            notification_types = {
                NotificationConfig.NotificationTypeEnum[type_str] for type_str in type_strings
            }
            to_add = notification_types - current_configs
            to_remove = current_configs - notification_types
            NotificationConfigController.add(
                to_add, user, workspace, session=session, context=context
            )
            NotificationConfigController.remove(to_remove, user, workspace, session=session)

        return (
            session.query(NotificationConfig)
            .filter(
                NotificationConfig.user_id == user.id,
                NotificationConfig.workspace_id == workspace.id,
            )
            .all()
        )

    @staticmethod
    def ensure_can_set_notification_config(setter: User, workspace: Workspace, *, session):
        pass  # TODO

    @staticmethod
    def get_company_ids(records: list[NotificationConfig], *, session):
        session.add_all(records)
        workspaces = [record.workspace for record in records]
        companies: set[Company] = set()
        for workspace in workspaces:
            companies.update(workspace.companies)
        return {company.id for company in companies}
