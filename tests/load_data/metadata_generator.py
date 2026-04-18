import os
from collections import OrderedDict

from chalicelib.new.query.domain.cfdi_to_dict import CFDIDictFromXMLParser


def generate_data_to_append(cfdi_dict: OrderedDict, estatus: str = "1") -> str:
    uuid = cfdi_dict.get("Complemento")[0].get("TimbreFiscalDigital").get("@UUID")
    rfc_emisor = cfdi_dict.get("Emisor").get("@Rfc")
    nombre_emisor = cfdi_dict.get("Emisor").get("@Nombre")
    rfc_receptor = cfdi_dict.get("Receptor").get("@Rfc")
    nombre_receptor = cfdi_dict.get("Receptor").get("@Nombre")
    rfc_pac = cfdi_dict.get("Complemento")[0].get("TimbreFiscalDigital").get("@RfcProvCertif")
    fecha = cfdi_dict.get("@fecha") or cfdi_dict.get("@Fecha")
    fecha_cert = cfdi_dict.get("Complemento")[0].get("TimbreFiscalDigital").get("@FechaTimbrado")
    monto = cfdi_dict.get("@total") or cfdi_dict.get("@Total")
    comprobante = cfdi_dict.get("@TipoDeComprobante")
    estatus = estatus
    fecha_cancelacion = (
        (cfdi_dict.get("@fecha") or cfdi_dict.get("@Fecha")) if estatus == "0" else ""
    )

    return f"{uuid}~{rfc_emisor}~{nombre_emisor}~{rfc_receptor}~{nombre_receptor}~{rfc_pac}~{fecha}~{fecha_cert}~{monto}~{comprobante}~{estatus}~{fecha_cancelacion}\n"  # noqa: E501


def create_metadata(xml_content: list, company: str, estatus: str) -> str:
    os.makedirs("tests/load_data/metadata", exist_ok=True)

    file_path = f"tests/load_data/metadata/{company}.csv"

    with open(file_path, "w", encoding="utf-8") as f:
        f.write(
            "Uuid~RfcEmisor~NombreEmisor~RfcReceptor~NombreReceptor~RfcPac~FechaEmision~FechaCertificacionSat~Monto~EfectoComprobante~Estatus~FechaCancelacion\n"
        )
        for xml in xml_content:
            cfdi_dict = CFDIDictFromXMLParser.get_dict_from_xml(xml)
            data_to_append = generate_data_to_append(cfdi_dict, estatus)
            f.write(data_to_append)

    return file_path
