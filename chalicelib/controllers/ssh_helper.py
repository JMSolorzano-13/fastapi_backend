from fabric import Connection  # type: ignore


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
