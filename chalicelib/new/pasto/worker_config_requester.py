import json
from dataclasses import dataclass

from chalicelib.new.pasto.consumer import consume
from chalicelib.new.pasto.request import PastoRequestAuth


@dataclass
class ConfigRequester(PastoRequestAuth):
    endpoint: str
    api_route: str
    method = 1  # 1 = POST
    action_code = "contpaqi-add-get-server-configuration"
    request_api_route = "syncWorkerActions/add"

    def request_config(self, workspace_identifier: str, worker_id: str, authorization: str) -> None:
        self.authorization = authorization

        url = f"{self.url}/{self.request_api_route}"

        data = json.dumps(
            {
                "action_code": self.action_code,
                "parameters": {
                    "Receiver": {
                        "Endpoint": self.endpoint,
                        "ApiRoute": self.api_route,
                        "Method": self.method,
                        "Headers": [
                            {
                                "Key": "workspace_identifier",
                                "Value": workspace_identifier,
                            },
                            {
                                "Key": "worker_id",
                                "Value": worker_id,
                            },
                        ],
                    }
                },
            }
        )
        headers = self.headers()

        response = consume(
            url=url,
            headers=headers,
            data=data,
            debug_info={
                "workspace_identifier": workspace_identifier,
                "worker_id": worker_id,
            },
        )

        return response.json()["data"]
