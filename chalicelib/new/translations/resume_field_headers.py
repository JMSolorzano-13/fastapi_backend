from chalicelib.controllers.enums import ResumeType

resume_field_headers = {
    "count": "Conteo de CFDIs",
    "RetencionesIVAMXN": "Retención IVA",
    "RetencionesIEPSMXN": "Retención IEPS",
    "RetencionesISRMXN": "Retención ISR",
    "TrasladosIVAMXN": "Traslado IVA",
    "TrasladosIEPSMXN": "Traslado IEPS",
    "TrasladosISRMXN": "Traslado ISR",
    "ImpuestosRetenidos": "Total de retenciones",
    "SubTotalMXN": "Subtotal",
    "DescuentoMXN": "Descuento",
    "NetoMXN": "Neto",
    "TotalMXN": "Total",
}

payroll_resume_field_headers = {
    "Qty": "Conteo de CFDIs",
    "EmpleadosQty": "Número de empleados",
    "TotalPercepciones": "Percepciones",
    "TotalDeducciones": "Deducciones",
    "TotalOtrosPagos": "Otros pagos",
    "PercepcionesTotalSueldos": "Sueldo",
    "PercepcionesTotalGravado": "Gravado",
    "PercepcionesTotalExento": "Exento",
    "DeduccionesTotalImpuestosRetenidos": "ISR retenido",
    "DeduccionesTotalOtrasDeducciones": "Otras deducciones",
    "SubsidioCausado": "Subsidio causado",
    "NetoAPagar": "Neto a pagar",
    "OtrasPercepciones": "Otras percepciones",
    "AjusteISRRetenido": "Ajuste ISR retenido",
    "PercepcionesJubilacionPensionRetiro": "Jubilación-Pensión",
    "PercepcionesSeparacionIndemnizacion": "Separación-Indemnización",
}

payment_resume_field_headers = {
    "count": "Conteo de CFDIs",
    "PaymentRelatedCount": "# Relacionados",
    "BaseIVA16": "Base IVA 16%",
    "IVATrasladado16": "IVA 16%",
    "BaseIVA8": "Base IVA 8%",
    "IVATrasladado8": "IVA 8%",
    "BaseIVA0": "Base IVA 0%",
    "BaseIVAExento": "Base IVA exento",
    "TrasladosIVA": "Total IVA",
    "RetencionesIVA": "Retenciones IVA",
    "RetencionesISR": "Retenciones ISR",
    "RetencionesIEPS": "Retenciones IEPS",
    "Total": "Total",
    "total_docto_relacionados": "Total pagos relacionados",
}

headers_by_resume_type = {
    ResumeType.BASIC: resume_field_headers,
    ResumeType.N: payroll_resume_field_headers,
    ResumeType.P: payment_resume_field_headers,
}
