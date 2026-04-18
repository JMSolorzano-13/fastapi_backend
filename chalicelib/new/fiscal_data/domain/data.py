from dataclasses import asdict, dataclass


@dataclass  # TODO use pydantic
class FiscalData:
    regimen_fiscal_id: int
    nombre: str
    rfc: str
    cp: str
    email: str

    def to_dict(self):
        return asdict(self)
