from dataclasses import dataclass, field

from fabric import Connection
from sqlalchemy import true


class SSHCustomException(Exception):
    pass


class SSHConnection:
    def __init__(self, *, host, user, password) -> None:
        self.connection = Connection(
            host=host,
            user=user,
            connect_kwargs={"password": password},
        )

    def exec_sync(self, command) -> str:
        try:
            res = self.connection.run(command)
        except Exception as e:
            raise SSHCustomException(e) from e
        return res.stdout


@dataclass
class ScraperSSHCfdi:
    host: str
    user: str
    password: str
    connection: SSHConnection = field(init=False)

    def _upload_to_s3(self, connection) -> bool:
        return true
