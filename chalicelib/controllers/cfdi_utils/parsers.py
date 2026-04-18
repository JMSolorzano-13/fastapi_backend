import csv  # noqa E501
import json
from datetime import datetime
from typing import Any


from chalicelib.logger import log, WARNING, EXCEPTION
from chalicelib.modules import Modules
from chalicelib.new.query.domain.cfdi_to_dict import CFDIDict, ComplementoDict
from chalicelib.new.shared.domain.primitives import Identifier, normalize_identifier
from chalicelib.schema.models.tenant import CFDI
from chalicelib.new.shared.domain.enums import Tax, TaxFactor
from chalicelib.new.utils import dicts

DECIMAL_PLACES = 6


def _round(value: float) -> float:
    return round(value, DECIMAL_PLACES)


class CFDIException(Exception):
    cfdi: str

    def __ini__(self, message, cfdi, *args, **kwargs):
        super().__init__(message, *args, **kwargs)
        self.cfdi = cfdi

    def __repr__(self) -> str:
        return f"{super().__repr__()}\nCFDI: {self.cfdi}"


class InvalidCFDI(CFDIException):
    pass


class NotSupportedCFDI(CFDIException):
    pass


meses_dict = {13: 2, 14: 4, 15: 6, 16: 8, 17: 10, 18: 12}


def get_tfd(cfdi_dict: CFDIDict) -> ComplementoDict:
    tfd = get_complementos(cfdi_dict, "TimbreFiscalDigital")
    if not tfd:
        raise InvalidCFDI("No TimbreFiscalDigital found")
    return tfd[0]


def ensure_float(value: Any, default=0) -> float:
    if not value:
        return default
    try:
        return float(value)
    except ValueError:
        log(
            Modules.PROCESS_XML,
            WARNING,
            "INVALID_FLOAT",
            {
                "value": value,
                "default": default,
            },
        )
        return default


def ensure_list(value):
    return value if isinstance(value, list) else [value]


def _set_tipo_cambio(cfdi: CFDI, value_str: str | None):
    value = ensure_float(value_str, default=1)
    cfdi.TipoCambio = value


possible_datetime_replacements = {
    " ": "T",
    "Z": "",
}


def normalize_fecha(date_str: str) -> datetime:
    date_str = date_str.replace("T", " ").split(".")[0]
    date_str = date_str.upper()
    for old, new in possible_datetime_replacements.items():
        date_str = date_str.replace(old, new)
    return datetime.fromisoformat(date_str)


def get_uuid(cfdi_dict: CFDIDict) -> Identifier:
    return normalize_identifier(get_tfd(cfdi_dict)["@UUID"])


def parser_33(cfdi_dict: CFDIDict) -> CFDI:
    cfdi = CFDI(
        UUID=get_uuid(cfdi_dict),
        Fecha=datetime.fromisoformat(cfdi_dict.get("@Fecha")),
        Total=ensure_float(cfdi_dict.get("@Total")),
        Folio=cfdi_dict.get("@Folio"),
        Serie=cfdi_dict.get("@Serie"),
        NoCertificado=cfdi_dict.get("@NoCertificado"),
        Certificado=cfdi_dict.get("@Certificado"),
        TipoDeComprobante=cfdi_dict.get("@TipoDeComprobante"),
        LugarExpedicion=cfdi_dict.get("@LugarExpedicion"),
        FormaPago=cfdi_dict.get("@FormaPago"),
        MetodoPago=cfdi_dict.get("@MetodoPago"),
        Moneda=cfdi_dict.get("@Moneda"),
        SubTotal=ensure_float(cfdi_dict.get("@SubTotal")),
        RfcEmisor=cfdi_dict["Emisor"]["@Rfc"],
        NombreEmisor=cfdi_dict["Emisor"].get("@Nombre"),
        RfcReceptor=cfdi_dict["Receptor"]["@Rfc"],
        NombreReceptor=cfdi_dict["Receptor"].get("@Nombre"),
        RfcPac=get_tfd(cfdi_dict)["@RfcProvCertif"],
        FechaCertificacionSat=datetime.fromisoformat(get_tfd(cfdi_dict)["@FechaTimbrado"]),
        Conceptos=json.dumps(cfdi_dict["Conceptos"]),
        Version=cfdi_dict.get("@Version"),
        Sello=cfdi_dict.get("@Sello"),
        UsoCFDIReceptor=cfdi_dict["Receptor"]["@UsoCFDI"],
        RegimenFiscalEmisor=cfdi_dict["Emisor"]["@RegimenFiscal"],
        CondicionesDePago=cfdi_dict.get("@CondicionesDePago"),
        CfdiRelacionados=json.dumps(cfdi_dict.get("CfdiRelacionados", {})),
        Impuestos=json.dumps(cfdi_dict.get("Impuestos", {})),
        Descuento=ensure_float(cfdi_dict.get("@Descuento")),
        NoCertificadoSAT=get_tfd(cfdi_dict)["@NoCertificadoSAT"],
        SelloSAT=get_tfd(cfdi_dict)["@SelloSAT"],
    )
    _set_fecha_filtro(cfdi, cfdi_dict)
    _set_tipo_cambio(cfdi, cfdi_dict.get("@TipoCambio"))
    return cfdi


def get_all_complementos(cfdi_dict: CFDIDict) -> dict[str, list[ComplementoDict]]:
    complementos = {}
    for complemento_group in cfdi_dict.get("Complemento", []):
        for complemento_name, complemento in complemento_group.items():
            if complemento_name not in complementos:
                complementos[complemento_name] = []
            if isinstance(complemento, dict):
                complementos[complemento_name].append(complemento)
            else:
                complementos[complemento_name].extend(complemento)
    return complementos


def get_complementos(cfdi_dict: CFDIDict, complemento_name: str) -> list[ComplementoDict]:
    """Get all occurrences of a complemento"""
    complementos = []
    for complemento_group in cfdi_dict.get("Complemento", []):
        complemento = complemento_group.get(complemento_name)
        if not complemento:
            continue
        if isinstance(complemento, list):
            complementos.extend(complemento)
        else:
            complementos.append(complemento)
    return complementos


def _get_meses_year(cfdi_dict: CFDIDict) -> [int, int]:
    meses = cfdi_dict.get("InformacionGlobal", {}).get("@Meses")
    year = cfdi_dict.get("InformacionGlobal", {}).get("@Año")
    meses = int(meses) if meses else None
    year = int(year) if year else None

    return meses, year


def is_publico_general(cfdi: CFDI, meses: int, year: int):
    return (
        cfdi.RfcReceptor == "XAXX010101000"
        and cfdi.Version == "4.0"
        and cfdi.TipoDeComprobante == "I"
        and cfdi.Fecha > datetime(2021, 12, 31, 23, 59, 59)
        and meses
        and year
    )


def get_date_by_bimester(meses: int):
    return meses_dict[meses]


def _set_fecha_filtro(cfdi: CFDI, cfdi_dict: CFDIDict):
    """Get the fechaFiltro from the CFDI dict"""
    nominas = get_complementos(cfdi_dict, "Nomina")
    pagos_complementos = get_complementos(cfdi_dict, "Pagos")
    meses, year = _get_meses_year(cfdi_dict)
    if not nominas and not pagos_complementos:
        if is_publico_general(cfdi, meses, year):
            if meses < 13:
                cfdi.FechaFiltro = datetime(year, meses, 1)
                cfdi.PaymentDate = datetime(year, meses, 1)
            else:
                cfdi.FechaFiltro = datetime(year, get_date_by_bimester(meses), 1)
                cfdi.PaymentDate = datetime(year, get_date_by_bimester(meses), 1)
        else:
            cfdi.FechaFiltro = cfdi.Fecha
            cfdi.PaymentDate = cfdi.Fecha
        return

    if nominas:
        if cfdi.Version == "3.2" and cfdi.TipoDeComprobante == "E":
            cfdi.active = False
        first_nomina = nominas[0]
        cfdi.FechaFiltro = _get_FechaFiltro_from_nomina(first_nomina, default=cfdi.Fecha)
        cfdi.PaymentDate = cfdi.FechaFiltro
        return

    first_pagos = pagos_complementos[0]
    try:
        cfdi.FechaFiltro = _get_FechaFiltro_from_Pagos(first_pagos)
        cfdi.PaymentDate = cfdi.FechaFiltro
    except ValueError as e:
        log(
            Modules.PROCESS_XML,
            EXCEPTION,
            "NO_FECHA_FILTRO_FROM_PAGOS",
            {
                "pagos": first_pagos,
                "cfdi": cfdi_dict,
                "exception": e,
            },
        )
        raise InvalidCFDI(str(e), cfdi_dict, first_pagos) from e


def _get_FechaFiltro_from_Pagos(pagos: dict[str, Any]) -> datetime:
    if not pagos.get("Pago"):
        raise ValueError("No `Pago` node found")
    pago_list = pagos["Pago"]
    if not isinstance(pago_list, list):
        pago_list = [pago_list]
    fecha_pago = str(pago_list[0]["@FechaPago"]).strip()
    return datetime.fromisoformat(fecha_pago)


def _get_FechaFiltro_from_nomina(nomina: dict[str, Any], default: datetime) -> datetime:
    try:
        return datetime.fromisoformat(nomina["@FechaPago"])
    except (KeyError, ValueError):
        return default


def parser_40(cfdi_dict: CFDIDict) -> CFDI:
    cfdi = parser_33(cfdi_dict)
    cfdi.Exportacion = cfdi_dict["@Exportacion"]
    if informacion_global := cfdi_dict.get("InformacionGlobal"):
        cfdi.Periodicidad = informacion_global["@Periodicidad"]
        cfdi.Meses = informacion_global["@Meses"]
        cfdi.Year = informacion_global["@Año"]
    cfdi.DomicilioFiscalReceptor = cfdi_dict["Receptor"]["@DomicilioFiscalReceptor"]
    cfdi.RegimenFiscalReceptor = cfdi_dict["Receptor"]["@RegimenFiscalReceptor"]
    # ACuentaTerceros # TODO
    return cfdi


supported = {
    "3.3": parser_33,
    "4.0": parser_40,
}


def get_pago_list(cfdi_dict: dict) -> list:
    supported_versions = {
        "1.0",
        "2.0",
    }
    pagos_complementos = get_complementos(cfdi_dict, "Pagos")
    res = []
    for pagos in pagos_complementos:
        if pagos.get("@Version") not in supported_versions:
            continue
        for pago in pagos["Pago"]:
            normalize_pago(pago)
            res.append(pago)
    return res


def normalize_pago(pago: dict[str, Any]) -> None:
    float_fields = {
        "@Monto",
        "@TipoCambioP",
    }
    for field in float_fields:
        if field in pago:
            pago[field] = ensure_float(pago[field])
    for docto_relacionado in pago.get("DoctoRelacionado", []):
        normalize_docto_relacionado(docto_relacionado)


def normalize_docto_relacionado(docto_relacionado: dict[str, Any]) -> None:
    float_fields = {
        "@ImpSaldoAnt",
        "@ImpPagado",
        "@ImpSaldoInsoluto",
        "@EquivalenciaDR",
    }
    float_tax_fields = {
        "@BaseDR",
        "@TasaOCuotaDR",
        "@ImporteDR",
    }
    for field in float_fields:
        if field in docto_relacionado:
            docto_relacionado[field] = ensure_float(docto_relacionado.get(field))
    traslados = dicts.get_from_dot_path(docto_relacionado, "ImpuestosDR.TrasladosDR.TrasladoDR", [])
    for traslado in traslados:
        for field in float_tax_fields:
            if field in traslado:
                traslado[field] = ensure_float(traslado.get(field))
    retenciones = dicts.get_from_dot_path(
        docto_relacionado, "ImpuestosDR.RetencionesDR.RetencionDR", []
    )
    for retencion in retenciones:
        for field in float_tax_fields:
            if field in retencion:
                retencion[field] = ensure_float(retencion.get(field))


def parser_pagos(cfdi_dict: CFDIDict, cfdi: CFDI) -> None:
    cfdi.pago_list = get_pago_list(cfdi_dict)


def get_nominas_if_exists(cfdi_dict: CFDIDict) -> dict[str, Any] | None:
    return cfdi_dict["Complemento"].get("Nomina")


def parser_nomina(cfdi_dict: CFDIDict, cfdi: CFDI) -> None:
    nominas = get_complementos(cfdi_dict, "Nomina")
    if not nominas:
        return
    cfdi.Nominas = nominas


complementos_supported = {
    "Pagos": parser_pagos,
    "Nomina": parser_nomina,
}


def process_complementos(cfdi_dict: CFDIDict, cfdi: CFDI):
    complementos = get_all_complementos(cfdi_dict)
    for complemento_name, complemento_parser in complementos_supported.items():
        if complemento_name in complementos:
            complemento_parser(cfdi_dict, cfdi)


def compute_mxn_fields(cfdi: CFDI):
    # TODO ignore in Pagos
    fields = {
        "Total": "TotalMXN",
        "SubTotal": "SubTotalMXN",
        "Neto": "NetoMXN",
        "Descuento": "DescuentoMXN",
        "TrasladosIVA": "TrasladosIVAMXN",
        "TrasladosIEPS": "TrasladosIEPSMXN",
        "TrasladosISR": "TrasladosISRMXN",
        "RetencionesIVA": "RetencionesIVAMXN",
        "RetencionesIEPS": "RetencionesIEPSMXN",
        "RetencionesISR": "RetencionesISRMXN",
    }
    for field, mxn_field in fields.items():
        tipo_cambio = float(cfdi.TipoCambio or 1)
        current = float(getattr(cfdi, field) or 0)
        converted = current * tipo_cambio
        setattr(cfdi, mxn_field, _round(converted))


def add_more_info(cfdi: CFDI, company_rfc: str) -> None:
    """Add extra info in the CFDI based on the existing one"""
    cfdi.Neto = _round(cfdi.SubTotal - cfdi.Descuento)
    cfdi.is_issued = cfdi.RfcEmisor == company_rfc
    cfdi.ExcludeFromIVA = cfdi.auto_exclude_iva
    cfdi.ExcludeFromISR = cfdi.auto_exclude_isr


def is_cancelled_other_month(cfdi: CFDI) -> bool:
    """Check if the CFDI is cancelled in the same month of the current one"""
    if not cfdi.FechaCancelacion:
        return False
    date = ensure_date(cfdi.Fecha)
    cancel_date = ensure_date(cfdi.FechaCancelacion)
    return date.month != cancel_date.month or date.year != cancel_date.year


def ensure_date(date: str | datetime) -> datetime:
    if isinstance(date, str):
        return datetime.fromisoformat(date)
    if isinstance(date, datetime):
        return date
    raise ValueError(f"Invalid date: {date}")


def add_validations(cfdi: CFDI) -> None:
    cfdi.TipoDeComprobante_I_MetodoPago_PUE = (
        cfdi.TipoDeComprobante == "I" and cfdi.MetodoPago == "PUE" and cfdi.FormaPago == "99"
    )
    cfdi.TipoDeComprobante_E_CfdiRelacionados_None = (
        cfdi.TipoDeComprobante == "E" and not json.loads(cfdi.CfdiRelacionados or "{}")
    )
    cfdi.TipoDeComprobante_I_MetodoPago_PPD = False
    cfdi.TipoDeComprobante_E_MetodoPago_PPD = False


def _compute_taxes(taxes_dict: dict[str, Any], tax_type: str) -> dict[str, float]:
    taxes_list = ensure_list(taxes_dict.get(f"{tax_type}", []))
    codes = {tax["@Impuesto"] for tax in taxes_list}
    return {
        code: _round(
            sum(float(tax.get("@Importe", 0)) for tax in taxes_list if tax["@Impuesto"] == code)
        )
        for code in codes
    }


def compute_impuestos(cfdi: CFDI, cfdi_dict: CFDIDict) -> None:
    if cfdi.TipoDeComprobante == "P":
        _compute_impuestos_pago(cfdi, cfdi_dict)
    else:
        _compute_impuestos(cfdi)


def _compute_impuestos_pago(cfdi: CFDI, cfdi_dict: CFDIDict) -> None:
    pagos_complementos = get_complementos(cfdi_dict, "Pagos")
    if not pagos_complementos:
        return
    first_pagos = pagos_complementos[0]

    def _get_totales_attribute(attribute: str) -> float:
        return float(first_pagos.get("Totales", {}).get(attribute, 0))

    def _get_pago_attribute(attribute: str) -> str:
        return first_pagos.get("Pago", [{}])[0].get(attribute)

    cfdi.TrasladosIVA = (
        _get_totales_attribute("@TotalTrasladosImpuestoIVA0")
        + _get_totales_attribute("@TotalTrasladosImpuestoIVA8")
        + _get_totales_attribute("@TotalTrasladosImpuestoIVA16")
    )
    cfdi.IVATrasladado8 = _get_totales_attribute("@TotalTrasladosImpuestoIVA8")
    cfdi.IVATrasladado16 = _get_totales_attribute("@TotalTrasladosImpuestoIVA16")
    cfdi.BaseIVA16 = _get_totales_attribute("@TotalTrasladosBaseIVA16")
    cfdi.BaseIVA8 = _get_totales_attribute("@TotalTrasladosBaseIVA8")
    cfdi.BaseIVA0 = _get_totales_attribute("@TotalTrasladosBaseIVA0")
    cfdi.BaseIVAExento = _get_totales_attribute("@TotalTrasladosBaseIVAExento")
    cfdi.RetencionesIVA = _get_totales_attribute("@TotalRetencionesIVA")
    cfdi.RetencionesIEPS = _get_totales_attribute("@TotalRetencionesIEPS")
    cfdi.RetencionesISR = _get_totales_attribute("@TotalRetencionesISR")

    cfdi.Total = _get_totales_attribute("@MontoTotalPagos")

    cfdi.Moneda = _get_pago_attribute("@MonedaP")  # TODO: WIP


def _set_iva_details(cfdi, traslados_node):
    traslados = ensure_list(traslados_node.get("Traslado", []))
    tolerancia = 1e-9
    cfdi.BaseIVA16 = sum(
        float(traslado.get("@Base", 0))  # 4.0
        for traslado in traslados
        if traslado["@Impuesto"] == Tax.IVA and ensure_float(traslado.get("@TasaOCuota")) == 0.16
    )
    cfdi.BaseIVA8 = sum(
        float(traslado.get("@Base", 0))  # 4.0
        for traslado in traslados
        if traslado["@Impuesto"] == Tax.IVA and ensure_float(traslado.get("@TasaOCuota")) == 0.08
    )
    cfdi.BaseIVA0 = sum(
        float(traslado.get("@Base", 0))  # 4.0
        for traslado in traslados
        if traslado["@Impuesto"] == Tax.IVA
        and traslado.get("@TasaOCuota") is not None
        and abs(float(traslado.get("@TasaOCuota"))) <= tolerancia
    )
    cfdi.BaseIVAExento = sum(
        float(traslado.get("@Base", 0))  # 4.0
        for traslado in traslados
        if traslado["@Impuesto"] == Tax.IVA and traslado.get("@TipoFactor") == TaxFactor.EXENTO
    )
    cfdi.IVATrasladado16 = sum(
        float(traslado["@Importe"])
        for traslado in traslados
        if traslado["@Impuesto"] == Tax.IVA and ensure_float(traslado.get("@TasaOCuota")) == 0.16
    )
    cfdi.IVATrasladado8 = sum(
        float(traslado["@Importe"])
        for traslado in traslados
        if traslado["@Impuesto"] == Tax.IVA and ensure_float(traslado.get("@TasaOCuota")) == 0.08
    )

    tipo_cambio = float(cfdi.TipoCambio or 1)

    cfdi.BaseIVA16 = round(cfdi.BaseIVA16 * tipo_cambio, 2)
    cfdi.BaseIVA8 = round(cfdi.BaseIVA8 * tipo_cambio, 2)
    cfdi.BaseIVA0 = round(cfdi.BaseIVA0 * tipo_cambio, 2)
    cfdi.BaseIVAExento = round(cfdi.BaseIVAExento * tipo_cambio, 2)
    cfdi.IVATrasladado16 = round(cfdi.IVATrasladado16 * tipo_cambio, 2)
    cfdi.IVATrasladado8 = round(cfdi.IVATrasladado8 * tipo_cambio, 2)


def _compute_impuestos(cfdi: CFDI) -> None:
    impuestos = json.loads(cfdi.Impuestos)
    if not impuestos:
        return
    traslados = impuestos.get("Traslados", {})
    retenciones = impuestos.get("Retenciones", {})
    traslados_sum = _compute_taxes(traslados, "Traslado")
    retenciones_sum = _compute_taxes(retenciones, "Retencion")
    cfdi.TrasladosISR = traslados_sum.get(Tax.ISR, 0)
    cfdi.TrasladosIVA = traslados_sum.get(Tax.IVA, 0)
    cfdi.TrasladosIEPS = traslados_sum.get(Tax.IEPS, 0)
    cfdi.RetencionesISR = retenciones_sum.get(Tax.ISR, 0)
    cfdi.RetencionesIVA = retenciones_sum.get(Tax.IVA, 0)
    cfdi.RetencionesIEPS = retenciones_sum.get(Tax.IEPS, 0)
    _set_iva_details(cfdi, traslados)
