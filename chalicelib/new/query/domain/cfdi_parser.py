from dataclasses import dataclass

from chalicelib.controllers.cfdi_utils import parsers
from chalicelib.new.query.domain.cfdi_to_dict import CFDIDict, CFDIDictFromXMLParser
from chalicelib.schema.models.tenant import CFDI as CFDIORM


def get_value_ignore_case(d, key, default=None):
    return d.get(key, d.get(key.lower(), d.get(key.upper(), default)))


@dataclass
class CFDIParserInvalidVersion(CFDIDictFromXMLParser.CFDIFromXMLException):
    version: str
    cfdi_dict: CFDIDict


class CFDIFromXMLParser:
    CFDIFromXMLException = CFDIDictFromXMLParser.CFDIFromXMLException
    CFDIParserInvalidXML = CFDIDictFromXMLParser.CFDIParserInvalidXML

    @classmethod
    def cfdi_from_xml(cls, xml_content: str, company_rfc: str) -> CFDIORM:
        cfdi_dict = CFDIDictFromXMLParser.get_dict_from_xml(xml_content)
        cfdi = cls.cfdi_from_dict(cfdi_dict, company_rfc)
        xml_content = xml_content.replace("\ufeff", "")  # TODO explain
        cfdi.xml_content = xml_content
        return cfdi

    @classmethod
    def cfdi_from_dict(cls, cfdi_dict: CFDIDict, company_rfc: str) -> CFDIORM:
        version = get_value_ignore_case(cfdi_dict, "@Version")
        parser = parsers.supported.get(version)
        if not parser:
            raise CFDIParserInvalidVersion(version, cfdi_dict)
        standard_cfdi: CFDIORM = parser(cfdi_dict)
        parsers.process_complementos(cfdi_dict, standard_cfdi)
        parsers.add_more_info(standard_cfdi, company_rfc)
        standard_cfdi.from_xml = True
        parsers.add_validations(standard_cfdi)
        parsers.compute_impuestos(standard_cfdi, cfdi_dict)
        parsers.compute_mxn_fields(standard_cfdi)
        # TODO update fields
        return standard_cfdi
