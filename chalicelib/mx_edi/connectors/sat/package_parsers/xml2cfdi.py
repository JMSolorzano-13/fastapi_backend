import logging
from collections.abc import Callable
from datetime import datetime
from typing import Any
from xml.etree import ElementTree
from xml.etree.ElementTree import Element
from zipfile import ZipFile

from ....core import CFDI, Concepto
from .cfdi_parser import CFDIParser, MissingData
from .utils import get_attr

_logger = logging.getLogger(__name__)

CFDI_NS = {
    "3.2": "{http://www.sat.gob.mx/cfd/3}",
    "3.3": "{http://www.sat.gob.mx/cfd/3}",
    "4.0": "{http://www.sat.gob.mx/cfd/4}",
}
TFD_NS = "{http://www.sat.gob.mx/TimbreFiscalDigital}"

tax_code_2_name = {
    "001": "ISR",
    "002": "IVA",
    "003": "IEPS",
}
tax_types = {
    "Traslados": "Traslado",
    "Retenciones": "Retencion",
}
all_taxes = {
    f"{tax_type}{tax_name}" for tax_type in tax_types for tax_name in tax_code_2_name.values()
}


def str_to_datetime(datetime_str: str) -> datetime:
    datetime_str = datetime_str[:19]  # Remove `Z` at the end
    return datetime.fromisoformat(datetime_str)


class XML2CFDI(CFDIParser):
    root_elements: dict[str, Callable] = {
        "Version": str,
        "Sello": str,
        "CondicionesDePago": str,
        "Folio": str,
        "Serie": str,
        "NoCertificado": str,
        "Certificado": str,
        "TipoDeComprobante": str,
        "Fecha": str_to_datetime,
        "LugarExpedicion": str,
        "FormaPago": str,
        "MetodoPago": str,
        "Moneda": str,
        "TipoCambio": float,
        "SubTotal": float,
        "Total": float,
        "Exportacion": str,
        "Periodicidad": str,
        "Meses": str,
    }

    @classmethod
    def _get_root_data(cls, xml: Element) -> dict[str, Any]:
        data = {}
        for field, caster in cls.root_elements.items():
            attr = get_attr(xml, field)
            if not attr:
                continue
            try:
                data[field] = caster(get_attr(xml, field))
            except ValueError:
                version = get_attr(xml, "Version")
                ns = CFDI_NS[version]
                complemento = xml.find(f"{ns}Complemento")
                if not complemento:
                    continue
                uuid = get_attr(
                    complemento.find(f"{TFD_NS}TimbreFiscalDigital"),
                    "UUID",
                )
                _logger.warning("Invalid value `%s` for field `%s`. UUID: `%s`", attr, field, uuid)
        return data

    @classmethod
    def _get_impuestos(cls, concepto, ns: str) -> dict[str, float]:
        """Get the sum of the taxes in Concepto"""
        xml_impuestos = concepto.find(f"{ns}Impuestos")

        res: dict[str, float] = {tax: 0 for tax in all_taxes}

        if xml_impuestos is None:
            return res
        for type_group, type_name in tax_types.items():
            xml_tax_group = xml_impuestos.find(f"{ns}{type_group}")
            if xml_tax_group is None:
                continue
            xml_taxs = xml_tax_group.findall(f"{ns}{type_name}")
            if not xml_taxs:
                continue
            for xml_tax in xml_taxs:
                code = get_attr(xml_tax, "Impuesto")
                res[f"{type_group}{tax_code_2_name[code]}"] += float(
                    get_attr(xml_tax, "Importe", 0)
                )

        return res

    @classmethod
    def _get_conceptos(cls, xml: Element, ns: str) -> list[Concepto]:
        xml_conceptos = xml.find(f"{ns}Conceptos")
        if not xml_conceptos:
            return []
        return [
            Concepto(
                Descripcion=get_attr(concepto, "Descripcion"),
                Cantidad=float(get_attr(concepto, "Cantidad")),
                ValorUnitario=float(get_attr(concepto, "ValorUnitario")),
                Importe=float(get_attr(concepto, "Importe")),
                Descuento=float(get_attr(concepto, "Descuento", 0)),
                ObjetoImp=get_attr(concepto, "ObjetoImp"),
                ClaveProdServ=get_attr(concepto, "ClaveProdServ"),
                **cls._get_impuestos(concepto, ns),
            )
            for concepto in xml_conceptos.findall(f"{ns}Concepto")
        ]

    @classmethod
    def parse(cls, xml: Element, xml_string: str = None) -> CFDI:
        data = cls._get_root_data(xml)
        ns = CFDI_NS[data["Version"]]
        complemento = xml.find(f"{ns}Complemento")
        if not complemento:
            raise MissingData(f"{ns}Complemento")
        CfdiRelacionados = xml.find(f"{ns}CfdiRelacionados")
        if CfdiRelacionados:
            data["CfdiRelacionados"] = {
                get_attr(cfdi_relacionado, "UUID")
                for cfdi_relacionado in CfdiRelacionados.findall(f"{ns}CfdiRelacionado")
            }
        uuid = get_attr(
            complemento.find(f"{TFD_NS}TimbreFiscalDigital"),
            "UUID",
        )
        emisor = xml.find(f"{ns}Emisor")
        receptor = xml.find(f"{ns}Receptor")
        data["RfcEmisor"] = get_attr(emisor, "Rfc")
        data["NombreEmisor"] = get_attr(emisor, "Nombre")
        data["RegimenFiscalEmisor"] = get_attr(emisor, "RegimenFiscal")
        data["RfcReceptor"] = get_attr(receptor, "Rfc")
        data["NombreReceptor"] = get_attr(receptor, "Nombre")
        data["UsoCFDIReceptor"] = get_attr(receptor, "UsoCFDI")
        data["UUID"] = uuid
        data["Conceptos"] = cls._get_conceptos(xml, ns)
        data["xml"] = xml_string

        cfdi = CFDI(**data)

        tax_sums = {
            tax: sum(getattr(concepto, tax, 0) for concepto in cfdi.Conceptos) for tax in all_taxes
        }

        cfdi.add_extra(
            "computed",
            {
                **tax_sums,
                "Neto": sum(concepto.Importe for concepto in cfdi.Conceptos)
                - sum(concepto.Descuento for concepto in cfdi.Conceptos),
                "ImpuestosRetenidos": sum(
                    amount for tax, amount in tax_sums.items() if tax.startswith("Retencion")
                ),
            },
        )
        return cfdi

    @classmethod
    def _get_xmls(cls, files: list[str]) -> list[tuple[Element, str]]:
        return [(ElementTree.fromstring(xml_file), xml_file) for xml_file in files]

    @classmethod
    def parse_zip(cls, zipfile: ZipFile) -> list["CFDI"]:
        xml_files = cls._get_files(zipfile)
        xmls = cls._get_xmls(xml_files)
        return [cls.parse(xml[0], xml[1]) for xml in xmls]
