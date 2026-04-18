class _PastoError(Exception):
    def __init__(self, url: str, debug_info: dict = None):
        super().__init__()
        self.debug_info = debug_info

    def __str__(self):
        return f"{super().__str__()} {self.debug_info}"


class PastoTimeoutError(_PastoError):
    pass


class PastoInternalError(_PastoError):
    pass
