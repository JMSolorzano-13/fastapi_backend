from dataclasses import dataclass
from datetime import datetime


@dataclass
class Received:
    total: int
    processed: int


@dataclass
class Issued:
    total: int
    processed: int


@dataclass
class CFDIByDays:
    date: datetime
    status: str
    issued: Issued
    received: Received


@dataclass
class CFDIHistoric:
    start: datetime or None
    end: datetime or None
    status: str
    issued: Issued
    received: Received


@dataclass
class CFDIBinnacle:
    days: list[CFDIByDays]
    historic: CFDIHistoric
