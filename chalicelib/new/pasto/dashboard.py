import json
from dataclasses import dataclass

from chalicelib.new.pasto.consumer import consume
from chalicelib.new.pasto.request import PastoRequest


@dataclass
class Dashboard(PastoRequest):
    _token: str = None

    @property
    def token(self):
        return self._token

    def login(self, email: str, password: str):
        if not self.token:
            self._token = self._get_token(email, password)
        return self.token

    def _get_token(self, email: str, password: str) -> str:
        url = f"{self.url}/usuario/login"

        data = json.dumps({"email": email, "password": password})
        headers = self.headers()

        response = consume(
            url=url,
            headers=headers,
            data=data,
            debug_info={"email": email},
        )

        return response.json()["data"]
