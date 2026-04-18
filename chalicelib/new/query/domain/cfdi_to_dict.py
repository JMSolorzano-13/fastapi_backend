from collections import OrderedDict
from dataclasses import dataclass

import xmltodict

CFDIDict = OrderedDict
ComplementoDict = OrderedDict
CfdiRelacionados = OrderedDict

CFDI_LIST_NODES = {
    "Percepcion",
    "Deduccion",
    # Complementos
    "Complemento",
    # Pagos
    "Pagos",
    "Pago",
    "CfdiRelacionados",
    "CfdiRelacionado",
    "DoctoRelacionado",
    "TrasladoDR",
    "RetencionDR",
    # Nomina
    "Nomina",
    "OtroPago",
}


class NoNamespaces:
    def get(self, _key, _default):
        return None

    def __getitem__(self, _key):
        return None


class CFDIDictFromXMLParser:
    class CFDIFromXMLException(Exception):
        pass

    @dataclass
    class CFDIParserInvalidXML(CFDIFromXMLException):
        xml_content: str

    @classmethod
    def get_dict_from_xml(cls, xml_content: str) -> CFDIDict:
        xml_dict = xmltodict.parse(
            xml_content,
            force_list=CFDI_LIST_NODES,
            namespaces=NoNamespaces(),
        )
        if comprobante := xml_dict.get("Comprobante"):
            return comprobante
        raise cls.CFDIParserInvalidXML(xml_content)
