import json
from dataclasses import dataclass

from chalicelib.new.pasto.consumer import consume
from chalicelib.new.pasto.domain.events import WorkerCredentialsSet
from chalicelib.new.pasto.request import PastoRequestAuth
from chalicelib.new.shared.domain.event.event_bus import EventBus
from chalicelib.new.shared.domain.event.event_type import EventType
from chalicelib.new.workspace.domain.workspace import Workspace
from chalicelib.new.workspace.infra import WorkspaceRepositorySA


@dataclass
class WorkerConfigurator(PastoRequestAuth):
    workspace_repo: WorkspaceRepositorySA
    bus: EventBus
    request_api_route = "worker/connector"
    connector_alias = "sql-001"

    def set_credentials(
        self,
        workspace: Workspace,
        worker_id: str,
        server: str,
        username: str,
        password: str,
    ):
        url = f"{self.url}/{self.request_api_route}"

        data = json.dumps(
            {
                "_id": worker_id,
                "connectors": [
                    {
                        "connector_alias": self.connector_alias,
                        "identifier": workspace.identifier,
                        "entries": [
                            {"key": "username", "value": username},
                            {"key": "password", "value": password},
                            {"key": "server", "value": server},
                        ],
                    }
                ],
            }
        )
        headers = self.headers()
        consume(
            url=url,
            headers=headers,
            data=data,
            debug_info={
                "connector_alias": self.connector_alias,
                "workspace_identifier": workspace.identifier,
                "worker_id": worker_id,
                "username": username,
                "server": server,
            },
        )

        workspace.pasto_installed = True
        self.workspace_repo.save(workspace)

        self.bus.publish(
            EventType.PASTO_WORKER_CREDENTIALS_SET,
            WorkerCredentialsSet(
                workspace_identifier=workspace.identifier,
                worker_id=worker_id,
                worker_token=workspace.pasto_worker_token,
            ),
        )
