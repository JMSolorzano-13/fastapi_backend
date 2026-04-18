import csv
from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator

from chalicelib.new.shared.domain.primitives import Identifier, normalize_identifier
from chalicelib.schema.models.tenant import CFDI as CFDIORM


class Metadata(BaseModel):
    model_config = ConfigDict(frozen=True)

    Uuid: Identifier
    RfcEmisor: str
    NombreEmisor: str
    RfcReceptor: str
    NombreReceptor: str
    RfcPac: str
    FechaEmision: datetime
    FechaCertificacionSat: datetime
    Monto: float
    TipoDeComprobante: str = Field(alias="EfectoComprobante")
    Estatus: bool
    FechaCancelacion: datetime | None

    url_xml: str | None = Field(None, repr=False)
    url_pdf: str | None = Field(None, repr=False)

    @field_validator("Uuid", mode="before")
    @classmethod
    def parse_uuid(cls, value: str) -> str:
        return normalize_identifier(value)

    @field_validator("Monto", mode="before")
    @classmethod
    def parse_monto(cls, value: str) -> float:
        if isinstance(value, int | float):
            return value
        return value.replace("$", "").replace(",", "")

    @field_validator("Estatus", mode="before")
    @classmethod
    def parse_estatus(cls, value: str) -> bool:
        return value in {"Vigente", 1, "1", "t"}

    @field_validator("FechaCancelacion", mode="before")
    @classmethod
    def parse_datetime(cls, value: str) -> datetime:
        return value or None

    @field_validator("TipoDeComprobante", mode="before")
    @classmethod
    def parse_tipo_de_comprobante(cls, value: str) -> str:
        return value[:1].upper()

    @field_validator("NombreReceptor", "NombreEmisor", mode="before")
    @classmethod
    def optional_str(cls, v):
        return v or ""

    @classmethod
    def custom_fields(cls) -> list[str]:
        res = [k for k in cls.model_fields if "_" not in k]

        for i, header in enumerate(res):
            if header == "TipoDeComprobante":
                res[i] = "EfectoComprobante"
        return res

    @classmethod
    def to_csv(cls, metadatas: list["Metadata"], file: Any) -> None:
        headers = cls.custom_fields()

        csv_writer = csv.DictWriter(file, delimiter="~", quoting=csv.QUOTE_NONE, fieldnames=headers)
        csv_writer.writeheader()

        for metadata in metadatas:
            row = metadata.model_dump(exclude={"url_xml", "url_pdf"}, by_alias=True)
            row["Uuid"] = row["Uuid"].upper()
            row["Estatus"] = int(row["Estatus"])
            row["NombreReceptor"] = row["NombreReceptor"].replace("\n", " ").replace('"', "")
            row["NombreEmisor"] = row["NombreEmisor"].replace("\n", " ").replace('"', "")
            total = int(row["Monto"])
            if total == row["Monto"]:
                row["Monto"] = total
            csv_writer.writerow(row)

    @classmethod
    def from_txt(cls, file_path: str) -> list["Metadata"]:
        """Parse a CFDIORM from a text"""
        METADATA_FILE_ROWS = 12

        headers = cls.custom_fields()

        metadatas = []
        uuids_processed = set()
        with open(file_path, encoding="UTF-8") as f:
            reader = csv.reader(f, delimiter="~", quoting=csv.QUOTE_NONE)
            try:
                next(reader)  # skip header
            except StopIteration:
                return metadatas

            tokens = []
            for row in reader:
                if len(row) == METADATA_FILE_ROWS - 1:  # Not FechaCancelacion
                    row.append("")
                if len(row) == METADATA_FILE_ROWS:
                    tokens = row
                else:
                    if tokens:
                        last_value = row.pop(0) if row else ""
                        tokens[-1] = (tokens[-1] or "") + last_value
                    tokens.extend(row)
                    if len(tokens) != METADATA_FILE_ROWS:
                        continue

                zipped = dict(zip(headers, tokens, strict=True))
                metadata = Metadata(**zipped)

                if metadata.Uuid in uuids_processed:
                    continue
                metadatas.append(metadata)
                uuids_processed.add(metadata.Uuid)

                tokens = []
        return metadatas

    @classmethod
    def from_cfdis(cls, cfdis: list[CFDIORM]) -> list["Metadata"]:
        return [Metadata.from_cfdi(cfdi) for cfdi in cfdis]

    @classmethod
    def from_cfdi(cls, cfdi: CFDIORM) -> "Metadata":
        return Metadata(
            Uuid=cfdi.UUID,
            RfcEmisor=cfdi.RfcEmisor,
            NombreEmisor=cfdi.NombreEmisor,
            RfcReceptor=cfdi.RfcReceptor,
            NombreReceptor=cfdi.NombreReceptor,
            RfcPac=cfdi.RfcPac,
            FechaEmision=cfdi.Fecha,
            Monto=cfdi.Total,  # TODO comprobar que sea el mismo monto que en metadata
            EfectoComprobante=cfdi.TipoDeComprobante,
            #
            FechaCertificacionSat=cfdi.Fecha,  # TODO parsear de XML
            Estatus=True,  # Si viene de un XML, debe ser vigente
            FechaCancelacion=None,  # No puede ser cancelado
        )
