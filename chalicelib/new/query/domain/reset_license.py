from dataclasses import dataclass

from chalicelib.new.pasto.consumer import PastoInternalError, consume
from chalicelib.new.pasto.domain.events.worker_created import WorkerCreated
from chalicelib.new.pasto.event.reset_add_license_key import ADDResetLicenseKey
from chalicelib.new.pasto.request import PastoRequestAuth
from chalicelib.new.shared.domain.event.event_bus import EventBus
from chalicelib.new.shared.domain.event.event_type import EventType
from chalicelib.new.workspace.infra.reset_license_repository_sa import LicenseRepositorySA


@dataclass
class Response:
    success: bool
    error: str
    error_code: int
    message: str
    data: bool


@dataclass
class ResetLicense(PastoRequestAuth):
    bus: EventBus
    reset_repo: LicenseRepositorySA

    def _reset_license(self, license_key: str) -> Response:
        url = f"{self.url}/{license_key}/reset"
        headers = self.headers()
        headers["Authorization"] = f"Bearer {self.authorization}"
        try:
            response = consume(
                url=url,
                headers=headers,
                data=license_key,
                debug_info={
                    "license_key": license_key,
                },
            )
        except PastoInternalError as e:
            return Response(
                success=False,
                error="error",
                error_code=e,
                message="",
                data=False,
            )

        self.bus.publish(
            EventType.PASTO_RESET_LICENSE_KEY_REQUESTED,
            ADDResetLicenseKey(
                license_key=license_key,
            ),
        )
        return Response(
            success=True,
            error="",
            error_code=response.status_code,
            message="",
            data=True,
        )

    def execute_queries(self, license_key: str):
        self.reset_repo.update_on_db(license_key)
        workspace = self.reset_repo.get_workspace_from_license_key(license_key)
        self.bus.publish(
            EventType.PASTO_WORKER_CREATED,
            WorkerCreated(
                workspace_identifier=workspace.identifier,
                worker_id=workspace.pasto_worker_id,
                worker_token=workspace.pasto_worker_token,
            ),
        )
