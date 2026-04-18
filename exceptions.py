"""Bridge module mapping Chalice exception names to FastAPI HTTPException subclasses.

Preserves the same class names used across controllers and domain/infra so that
existing business logic requires only an import-path change, not a code rewrite.
"""

from fastapi import HTTPException


class BadRequestError(HTTPException):
    def __init__(self, msg: str = "Bad Request"):
        super().__init__(status_code=400, detail=str(msg))


class UnauthorizedError(HTTPException):
    def __init__(self, msg: str = "Unauthorized"):
        super().__init__(status_code=401, detail=str(msg))


class ForbiddenError(HTTPException):
    def __init__(self, msg: str = "Forbidden"):
        super().__init__(status_code=403, detail=str(msg))


class NotFoundError(HTTPException):
    def __init__(self, msg: str = "Not Found"):
        super().__init__(status_code=404, detail=str(msg))


class MethodNotAllowedError(HTTPException):
    def __init__(self, msg: str = "Method Not Allowed"):
        super().__init__(status_code=405, detail=str(msg))


class ChaliceViewError(HTTPException):
    """Maps the old catch-all Chalice error to a 500."""

    def __init__(self, msg: str = "Internal Server Error"):
        super().__init__(status_code=500, detail=str(msg))
