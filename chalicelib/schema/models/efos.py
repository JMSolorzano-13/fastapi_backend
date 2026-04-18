import enum

from sqlalchemy import Column, Enum, Integer, String

from chalicelib.modules import NameEnum
from chalicelib.schema.models.model import SHARED_TENANT_SCHEMA_PLACEHOLDER, Model

EFOS_DATE_FORMAT_PSQL = "DD/MM/YYYY"
EFOS_DATE_FORMAT_PYTHON = "%d/%m/%Y"


class EFOS(Model):
    __tablename__ = "efos"
    __table_args__ = {"schema": SHARED_TENANT_SCHEMA_PLACEHOLDER}

    no = Column(
        Integer,
        index=True,
        nullable=False,
    )
    rfc = Column(
        String,
        index=True,
        nullable=False,
    )
    name = Column(
        String,
        index=True,
        nullable=False,
    )

    class StateEnum(NameEnum):
        DEFINITIVE = enum.auto()
        DISTORTED = enum.auto()
        ALLEGED = enum.auto()
        FAVORABLE_JUDGMENT = enum.auto()

    state = Column(
        Enum(StateEnum, name="stateenum"),
        index=True,
        nullable=False,
    )
    sat_office_alleged = Column(String)
    sat_publish_alleged_date = Column(String, nullable=False)
    dof_office_alleged = Column(String)
    dof_publish_alleged_date = Column(String)
    sat_office_distored = Column(String)
    sat_publish_distored_date = Column(String)
    dof_office_distored = Column(String)
    dof_publish_distored_date = Column(String)
    sat_office_definitive = Column(String)
    sat_publish_definitive_date = Column(String)
    dof_office_definitive = Column(String)
    dof_publish_definitive_date = Column(String)
    sat_office_favorable_judgement = Column(String)
    sat_publish_favorable_judgement_date = Column(String)
    dof_office_favorable_judgement = Column(String)
    dof_publish_favorable_judgement_date = Column(String)

    @property
    def human_readable_status(self):
        status = {
            "DEFINITIVE": "Definitivo",
            "DISTORTED": "Desvirtuado",
            "ALLEGED": "Presunto",
            "FAVORABLE_JUDGMENT": "Sentencia favorable",
        }
        current_state = status.get(self.state.name, "Otro")
        return current_state

    @staticmethod
    def _public_fields():
        return (
            "no",
            "rfc",
            "name",
            "state",
            "sat_office_alleged",
            "sat_publish_alleged_date",
            "dof_office_alleged",
            "dof_publish_alleged_date",
            "sat_office_distored",
            "sat_publish_distored_date",
            "dof_office_distored",
            "dof_publish_distored_date",
            "sat_office_definitive",
            "sat_publish_definitive_date",
            "dof_office_definitive",
            "dof_publish_definitive_date",
            "sat_office_favorable_judgement",
            "sat_publish_favorable_judgement_date",
            "dof_office_favorable_judgement",
            "dof_publish_favorable_judgement_date",
        )

    def __eq__(self, other):
        fields = self._public_fields()
        return all(getattr(self, field) == getattr(other, field) for field in fields)

    def copy(self, other):
        fields = self._public_fields()
        for field in fields:
            setattr(self, field, getattr(other, field))
