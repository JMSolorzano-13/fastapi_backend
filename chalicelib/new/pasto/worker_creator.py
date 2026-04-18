import json
from dataclasses import dataclass

from chalicelib.new.pasto.consumer import consume
from chalicelib.new.pasto.domain.events import WorkerCreated
from chalicelib.new.pasto.request import PastoRequestAuth
from chalicelib.new.shared.domain.event.event_bus import EventBus
from chalicelib.new.shared.domain.event.event_type import EventType
from chalicelib.new.workspace.domain import WorkspaceRepository
from chalicelib.new.workspace.domain.workspace import Workspace


@dataclass
class Worker:
    pasto_id: str
    serial_number: str
    token: str


@dataclass
class WorkerCreator(PastoRequestAuth):
    subscription_id: str
    dashboard_id: str
    workspace_repo: WorkspaceRepository
    bus: EventBus
    request_api_route = "worker"

    def _create_in_pasto(self, workspace_identifier: str) -> Worker:
        url = f"{self.url}/{self.request_api_route}"

        data = json.dumps(
            {
                "name": workspace_identifier,
                "description": workspace_identifier,
                "status": 1,
                "purchasedsubcription_id": self.subscription_id,
                "dashboard_id": self.dashboard_id,
            }
        )
        headers = self.headers()

        response = consume(
            url=url,
            headers=headers,
            data=data,
            debug_info={
                "workspace_identifier": workspace_identifier,
                "subscription_id": self.subscription_id,
                "dashboard_id": self.dashboard_id,
            },
        )

        data = response.json()["data"]
        return Worker(
            pasto_id=data["_id"],
            serial_number=data["serial_number"],
            token=data["api_keys"]["production"]["worker_token"],
        )

    def create(self, workspace: Workspace) -> Worker:
        worker = self._create_in_pasto(workspace.identifier)
        workspace.pasto_worker_id = worker.pasto_id
        workspace.pasto_license_key = worker.serial_number
        workspace.pasto_worker_token = worker.token
        self.workspace_repo.save(workspace)
        self.bus.publish(
            EventType.PASTO_WORKER_CREATED,
            WorkerCreated(
                workspace_identifier=workspace.identifier,
                worker_id=worker.pasto_id,
                worker_token=worker.token,
            ),
        )
        return worker
