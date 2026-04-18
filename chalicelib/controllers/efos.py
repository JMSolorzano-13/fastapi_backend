import csv
from datetime import date, datetime
from typing import Any

import requests
from bs4 import BeautifulSoup  # type: ignore
from sqlalchemy.orm import Session

from chalicelib.controllers import Domain
from chalicelib.controllers.common import CommonController
from chalicelib.schema.models import EFOS

SAT_FILE_URL = "http://omawww.sat.gob.mx/cifras_sat/Documents/Listado_Completo_69-B.csv"
EFOS_LAST_UPDATE_PARAM = "efos_last_update"
SAT_LAST_UPDATE_URL = "http://omawww.sat.gob.mx/cifras_sat/Paginas/datos/ListCompleta69B.html"
SAT_UPDATE_DATE_FORMAT = r"Información actualizada al %d de %m de %Y."

POSSIBLE_SAT_FILE_ENCODINGS = {
    "utf-8",
    "cp1252",
}

MONTH_TO_NUMBER = {
    "Enero": "01",
    "Febrero": "02",
    "Marzo": "03",
    "Abril": "04",
    "Mayo": "05",
    "Junio": "06",
    "Julio": "07",
    "Agosto": "08",
    "Septiembre": "09",
    "Octubre": "10",
    "Noviembre": "11",
    "Diciembre": "12",
}

NUMBER_TO_MONTH = {int(k): v for v, k in MONTH_TO_NUMBER.items()}


class SatEfosComunicationException(Exception):
    pass


class EFOSController(CommonController):
    model = EFOS
    fuzzy_fields = (
        EFOS.rfc,
        EFOS.name,
    )
    default_distinct = True

    @staticmethod
    def _get_updated_efos() -> list[EFOS]:
        data = EFOSController._get_data_from_sat()
        return EFOSController._get_efos_from_data(data)

    @staticmethod
    def str_to_state(state: str) -> EFOS.StateEnum:
        convertions = {
            "Definitivo": EFOS.StateEnum.DEFINITIVE,
            "Desvirtuado": EFOS.StateEnum.DISTORTED,
            "Presunto": EFOS.StateEnum.ALLEGED,
            "Sentencia Favorable": EFOS.StateEnum.FAVORABLE_JUDGMENT,
        }
        return convertions[state]

    @staticmethod
    def _get_efos_from_data(data: csv.DictReader) -> list[EFOS]:
        processors = {
            "no": int,
            "state": EFOSController.str_to_state,
        }
        col_rel = {
            "No": "no",
            "RFC": "rfc",
            "Nombre del Contribuyente": "name",
            "Situación del contribuyente": "state",
            "Número y fecha de oficio global de presunción SAT": "sat_office_alleged",
            "Publicación página SAT presuntos": "sat_publish_alleged_date",
            "Número y fecha de oficio global de presunción DOF": "dof_office_alleged",
            "Publicación DOF presuntos": "dof_publish_alleged_date",
            "Número y fecha de oficio global de contribuyentes que desvirtuaron SAT": "sat_office_distored",  # noqa E501
            "Publicación página SAT desvirtuados": "sat_publish_distored_date",
            "Número y fecha de oficio global de contribuyentes que desvirtuaron DOF": "dof_office_distored",  # noqa E501
            "Publicación DOF desvirtuados": "dof_publish_distored_date",
            "Número y fecha de oficio global de definitivos SAT": "sat_office_definitive",
            "Publicación página SAT definitivos": "sat_publish_definitive_date",
            "Número y fecha de oficio global de definitivos DOF": "dof_office_definitive",
            "Publicación DOF definitivos": "dof_publish_definitive_date",
            "Número y fecha de oficio global de sentencia favorable SAT": "sat_office_favorable_judgement",  # noqa E501
            "Publicación página SAT sentencia favorable": "sat_publish_favorable_judgement_date",
            "Número y fecha de oficio global de sentencia favorable DOF": "dof_office_favorable_judgement",  # noqa E501
            "Publicación DOF sentencia favorable": "dof_publish_favorable_judgement_date",
        }

        def proccess(k, v):
            if k not in processors:
                return v
            if not v:
                return None
            try:
                return processors[k](v)  # TODO
            except ValueError:
                return None

        def csv_to_efos_dict(row: dict[str, Any]) -> dict[str, Any]:
            d = {col_rel[key]: row[key] for key in row}
            return {k: proccess(k, v) for k, v in d.items()}

        return [EFOS(**csv_to_efos_dict(row)) for row in data]

    @staticmethod
    def _download_sat_csv_content() -> bytes:
        response = requests.get(SAT_FILE_URL)
        if response.status_code != 200:
            raise SatEfosComunicationException(
                f"Error downloading SAT file: {response.status_code}"
            )
        return response.content

    @staticmethod
    def _parse_csv_content(raw_content: bytes) -> csv.DictReader:
        """Parsea bytes del CSV a DictReader, omitiendo las primeras 2 líneas de encabezado."""
        content = decode_content(raw_content)

        data = content.splitlines()[2:]
        return csv.DictReader(data)

    @staticmethod
    def _get_data_from_sat() -> csv.DictReader:
        raw_content = EFOSController._download_sat_csv_content()
        return EFOSController._parse_csv_content(raw_content)

    @staticmethod
    def create_new_efos(efos_list: list[EFOS], session, context) -> None:
        session.add_all(efos_list)
        session.flush()

    @staticmethod
    def _get_date_from_sat() -> date:
        """Get date from SAT page"""

        def get_last_update_date_from_sat() -> str:
            """Get last update date from SAT."""
            response = requests.get(SAT_LAST_UPDATE_URL)
            if response.status_code != 200:
                raise SatEfosComunicationException(
                    f"Error downloading SAT file: {response.status_code}"
                )

            soup = BeautifulSoup(response.content, "html.parser")
            date_tr = soup.find_all("tr")[1]
            date_str = date_tr.text.replace("\n", "")
            for month, number in MONTH_TO_NUMBER.items():
                date_str = date_str.replace(month, number)
            return date_str

        date_from_sat_str = get_last_update_date_from_sat()
        return datetime.strptime(date_from_sat_str, SAT_UPDATE_DATE_FORMAT).date()

    @classmethod
    def update_from_sat(cls, *, session: Session, context=None):
        """Get the new EFOS from the SAT and create or update the current ones,
        notifying the changes
        Assumptions:
        1. An RFC can be more than once in the SAT file
        2. If an RFC is duplicated, they are side by side in the SAT file
        3. If an RFC is duplicated, the EFOS state is the one with the highest number
        """
        # date_from_sat = cls._get_date_from_sat()

        # def need_update(date_from_sat):
        #     last_update_str = ParamController.get_param(EFOS_LAST_UPDATE_PARAM)
        #     if not last_update_str:
        #         return True
        #     last_update = date.fromisoformat(last_update_str)
        #     return date_from_sat > last_update

        # if not need_update(date_from_sat):
        #     return
        session.query(EFOS).delete()  # TODO don't delete
        updated_efos_list = cls._get_updated_efos()
        cls.create_new_efos(updated_efos_list, session, context)

        # ParamController.set(EFOS_LAST_UPDATE_PARAM, date_from_sat.isoformat(), session=session)

    @staticmethod
    def resume(
        domain: Domain,
        fuzzy_search: str = None,
        *,
        session: Session,
        context,
        resume_type,
        fields: list[str] = None,
    ):
        res = {}
        for state in EFOS.StateEnum:
            _res, efos_count = EFOSController._search(
                domain=domain + [("state", "=", state)],
                fields=fields,
                fuzzy_search=fuzzy_search,
                session=session,
                need_count=True,
            )
            res[state.name] = efos_count
        return res


def decode_content(raw_content: bytes, encodes=POSSIBLE_SAT_FILE_ENCODINGS) -> str:
    for encoding in encodes:
        try:
            content = raw_content.decode(encoding)
            break
        except LookupError as e:
            raise SatEfosComunicationException(
                f"Invalid encoding '{encoding}' in POSSIBLE_SAT_FILE_ENCODINGS"
            ) from e
        except UnicodeDecodeError:
            continue
    else:
        raise SatEfosComunicationException(
            f"Could not decode SAT file with any of the supported encodings: "
            f"{POSSIBLE_SAT_FILE_ENCODINGS}"
        )

    return content
