from chalicelib.schema.models.tenant import Nomina as NominaORM


def clean_string(string):
    return string and string.strip()


class NominaParser:
    @classmethod
    def parse(cls, nomina_dicts, uuid, company_identifier) -> NominaORM:
        nd = nomina_dicts[0]

        subsidio = 0.0
        isr = 0.0
        for n in nomina_dicts:
            if otros_pagos := n.get("OtrosPagos", {}).get("OtroPago", []):
                subsidio += sum(
                    float(op.get("SubsidioAlEmpleo", {}).get("@SubsidioCausado", 0))
                    for op in otros_pagos
                )
                isr += sum(
                    float(op.get("@Importe", 0))
                    for op in otros_pagos
                    if op.get("@TipoOtroPago") in ("001", "004", "005")
                )

        sindicalizado = nd["Receptor"].get("@Sindicalizado")
        if sindicalizado is not None:
            sindicalizado = sindicalizado == "Sí"

        emisor = nd.get("Emisor") or {}

        return NominaORM(
            company_identifier=company_identifier,
            cfdi_uuid=uuid,
            Version=nd["@Version"],
            TipoNomina=nd["@TipoNomina"],
            FechaPago=nd["@FechaPago"],
            FechaInicialPago=nd["@FechaInicialPago"],
            FechaFinalPago=nd["@FechaFinalPago"],
            NumDiasPagados=nd["@NumDiasPagados"],
            TotalPercepciones=sum(float(n.get("@TotalPercepciones", 0)) for n in nomina_dicts),
            TotalDeducciones=sum(float(n.get("@TotalDeducciones", 0)) for n in nomina_dicts),
            TotalOtrosPagos=sum(float(n.get("@TotalOtrosPagos", 0)) for n in nomina_dicts),
            EmisorRegistroPatronal=clean_string(emisor.get("@RegistroPatronal")),
            ReceptorCurp=clean_string(nd["Receptor"]["@Curp"]),
            ReceptorNumSeguridadSocial=clean_string(nd["Receptor"].get("@NumSeguridadSocial")),
            ReceptorFechaInicioRelLaboral=clean_string(
                nd["Receptor"].get("@FechaInicioRelLaboral")
            ),
            ReceptorAntigüedad=clean_string(nd["Receptor"].get("@Antigüedad")),
            ReceptorTipoContrato=clean_string(nd["Receptor"]["@TipoContrato"]),
            ReceptorSindicalizado=sindicalizado,
            ReceptorTipoJornada=clean_string(nd["Receptor"].get("@TipoJornada")),
            ReceptorTipoRegimen=clean_string(nd["Receptor"].get("@TipoRegimen")),
            ReceptorNumEmpleado=clean_string(nd["Receptor"]["@NumEmpleado"]),
            ReceptorDepartamento=clean_string(nd["Receptor"].get("@Departamento")),
            ReceptorPuesto=clean_string(nd["Receptor"].get("@Puesto")),
            ReceptorRiesgoPuesto=clean_string(nd["Receptor"].get("@RiesgoPuesto")),
            ReceptorPeriodicidadPago=clean_string(nd["Receptor"]["@PeriodicidadPago"]),
            ReceptorBanco=clean_string(nd["Receptor"].get("@Banco")),
            ReceptorCuentaBancaria=clean_string(nd["Receptor"].get("@CuentaBancaria")),
            ReceptorSalarioBaseCotApor=clean_string(nd["Receptor"].get("@SalarioBaseCotApor")),
            ReceptorSalarioDiarioIntegrado=clean_string(
                nd["Receptor"].get("@SalarioDiarioIntegrado")
            ),
            ReceptorClaveEntFed=clean_string(nd["Receptor"]["@ClaveEntFed"]),  # TODO ensure in ENUM
            PercepcionesTotalSueldos=sum(
                float(n.get("Percepciones", {}).get("@TotalSueldos", 0)) for n in nomina_dicts
            ),
            PercepcionesTotalGravado=sum(
                float(n.get("Percepciones", {}).get("@TotalGravado", 0)) for n in nomina_dicts
            ),
            PercepcionesTotalExento=sum(
                float(n.get("Percepciones", {}).get("@TotalExento", 0)) for n in nomina_dicts
            ),
            PercepcionesSeparacionIndemnizacion=sum(
                float(n.get("Percepciones", {}).get("@TotalSeparacionIndemnizacion", 0))
                for n in nomina_dicts
            ),
            PercepcionesJubilacionPensionRetiro=sum(
                float(n.get("Percepciones", {}).get("@TotalJubilacionPensionRetiro", 0))
                for n in nomina_dicts
            ),
            DeduccionesTotalOtrasDeducciones=sum(
                float(n.get("Deducciones", {}).get("@TotalOtrasDeducciones", 0))
                for n in nomina_dicts
            ),
            DeduccionesTotalImpuestosRetenidos=sum(
                float(n.get("Deducciones", {}).get("@TotalImpuestosRetenidos", 0))
                for n in nomina_dicts
            ),
            SubsidioCausado=subsidio,
            AjusteISRRetenido=isr,
        )
